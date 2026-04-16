# 🚀 Quick Reference — Graph Query Engine

## Fastest Setup — Docker

```bash
cd demo

# Full stack (engine + Iceberg/Trino + Jupyter):
docker compose up

# H2 only (lighter, no Iceberg):
docker compose up engine jupyter
```

| Service | URL | Credentials |
|---------|-----|-------------|
| Jupyter notebooks | http://localhost:8888 | password: `GqeDemo123` |
| Engine REST API   | http://localhost:7000 | — |
| Trino (Iceberg)   | http://localhost:8080 | — |
| MinIO console     | http://localhost:9001 | `minioadmin` / `minioadmin` |

Open **`aml/notebooks/aml_sql_showcase.ipynb`** or **`social_networking/social_network_demo.ipynb`** and run cells top-to-bottom.

> **Kaggle data** (AML dataset) requires a Kaggle API token.  
> Set `KAGGLE_USERNAME` and `KAGGLE_KEY` before `docker compose up`,  
> or add them to `demo/.env`.

---

## Local Setup (Maven)

```bash
# Terminal 1 — start engine
mvn exec:java

# Terminal 2 — open notebook
jupyter notebook demo/aml/notebooks/aml_sql_showcase.ipynb
```

---

## Command Cheat Sheet

| Task | Command |
|------|---------|
| **Docker full stack** | `cd demo && docker compose up` |
| **Docker H2 only** | `cd demo && docker compose up engine jupyter` |
| **Docker stop** | `cd demo && docker compose down` |
| **Start engine (Maven)** | `mvn exec:java` |
| **Run all tests** | `mvn test` |
| **Open AML notebook** | `jupyter notebook demo/aml/notebooks/aml_sql_showcase.ipynb` |
| **Open Social notebook** | `jupyter notebook demo/social_networking/social_network_demo.ipynb` |
| **Run single cell** | Click cell + `Shift+Enter` |
| **Run all cells** | `Kernel → Restart & Run All` |
| **Health check** | `curl http://localhost:7000/health` |

---

## Notebook Execution Order

```
1. Setup & Configuration     ← set BACKEND, MAX_ROWS, WIPE
2. Health Check              ← verify engine is reachable
3. Data Loading (Step 1)     ← upload mapping + seed data
4. Query Cells               ← run top-to-bottom
```

---

## Minimal Working Example

```python
import requests

# Health check
r = requests.get("http://localhost:7000/health")
print(r.json())  # {"status":"ok","provider":"sql-multi",...}

# Run a Gremlin query
r = requests.post("http://localhost:7000/gremlin/query",
                  json={"gremlin": "g.V().count()"})
print(r.json())  # {"result":[[423684]]}
```

---

## AML Query Reference

### Simple Queries

```groovy
// Count accounts
g.V().hasLabel('Account').count()

// Count transfers
g.E().hasLabel('TRANSFER').count()

// Suspicious transfers
g.E().has('isLaundering','1').count()

// High-risk accounts (> 0.7)
g.V().hasLabel('Account').has('riskScore', gt(0.7))
  .project('accountId','bankId','riskScore')
  .by('accountId').by('bankId').by('riskScore')
  .order().by(select('riskScore'), Order.desc).limit(10)
```

### Complex / Multi-hop Queries

```groovy
// Top hub accounts (out-degree)
g.V().hasLabel('Account')
  .project('accountId','bankId','outDegree')
  .by('accountId').by('bankId')
  .by(outE('TRANSFER').count())
  .order().by(select('outDegree'), Order.desc).limit(15)

// 2-hop money trail (simplePath)
g.V().hasLabel('Account')
  .where(outE('TRANSFER').has('isLaundering','1'))
  .limit(1)
  .repeat(out('TRANSFER').simplePath()).times(2)
  .path().by('accountId').limit(10)

// 3-hop money trail
g.V().hasLabel('Account')
  .where(outE('TRANSFER').has('isLaundering','1'))
  .limit(1)
  .repeat(out('TRANSFER').simplePath()).times(3)
  .path().by('accountId').limit(10)

// 5-hop layering chain
g.V().hasLabel('Account')
  .where(outE('TRANSFER').has('isLaundering','1'))
  .limit(1)
  .repeat(out('TRANSFER').simplePath()).times(5)
  .path().by('accountId').limit(10)
```

---

## Social Network Query Reference

```groovy
// All people
g.V().hasLabel('Person').valueMap('name','age').limit(10)

// Friends of friends
g.V().hasLabel('Person').has('name','Alice')
  .repeat(out('KNOWS').simplePath()).times(2)
  .path().by('name').limit(10)

// Colleagues at same company
g.V().hasLabel('Person')
  .where(out('WORKS_AT').has('name','Acme'))
  .values('name').limit(10)
```

---

## Backend Switching (in notebooks)

```python
BACKEND = 'h2'       # H2 (local file DB, no Docker dependency)
BACKEND = 'iceberg'  # Iceberg via Trino (requires Docker Iceberg stack)
```

---

## Docker Image

The engine is published to Docker Hub:

```bash
docker pull vaijosh/graph-query-engine:latest
```

To rebuild locally after code changes:

```bash
mvn package -DskipTests
cd demo && docker compose up --build
```

---

## REST API Quick Reference

```bash
# Health
curl http://localhost:7000/health

# Execute Gremlin query
curl -X POST http://localhost:7000/gremlin/query \
  -H "Content-Type: application/json" \
  -d '{"gremlin":"g.V().hasLabel(\"Account\").count()"}'

# SQL explain (no execution)
curl -X POST http://localhost:7000/query/explain \
  -H "Content-Type: application/json" \
  -d '{"gremlin":"g.V().hasLabel(\"Account\").limit(5)"}'

# Upload mapping
curl -X POST http://localhost:7000/mapping/upload \
  -F "file=@demo/aml/mappings/aml-mapping.json" \
  -F "id=aml" \
  -F "activate=true"

# List mappings
curl http://localhost:7000/mappings

# Switch active mapping
curl -X POST "http://localhost:7000/mapping/active?id=aml"
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Connection refused | Engine not running — `mvn exec:java` or `cd demo && docker compose up` |
| `Schema not found` | Wrong mapping active — check `/mappings` and activate correct one |
| Zero results | Seed data not loaded — run Step 1 cell in notebook |
| Timeout error | Add `.limit(20)` to query, or increase `QUERY_TIMEOUT` in notebook |
| Jupyter token prompt | Use password `GqeDemo123` (Docker) |
| OOM on deep traversal | Reduce hops or set `WCOJ_MAX_EDGES=500000` |
| Iceberg tables missing | Run seed cell with `WIPE = True` after Iceberg stack is up |

---

## Performance Tips

- **Filter early** — add `.has()` before expensive traversals
- **Keep limits realistic** — `.limit(20)` not `.limit(1000000)`
- **Use simplePath()** — avoids cycle explosion in deep traversals
- **WCOJ is on by default** — handles multi-hop joins efficiently
- **Start small** — use `MAX_ROWS = 100_000` first, scale up after

---

## File Locations

```
graph-query-engine/
├── demo/
│   ├── docker-compose.yml               ← Docker full stack
│   ├── docker-mappings/                 ← Docker-specific mappings (trino:8080 URLs)
│   ├── aml/
│   │   ├── notebooks/aml_sql_showcase.ipynb
│   │   └── mappings/
│   ├── social_networking/
│   │   └── social_network_demo.ipynb
│   └── infra/scripts/                   ← Iceberg infra scripts
├── docker/
│   ├── Dockerfile.local                 ← Local Docker build
│   └── entrypoint.sh
└── data/                                ← CSV data files (not in git)
```

---

**For full details see: `README.md` · `demo/DEMO_GUIDE.md` · `DESIGN_DOCUMENT.md`**
