# Demo Guide — Graph Query Engine

This guide walks you through launching the engine and running the two demo notebooks.

---

## Prerequisites

| Tool | Version |
|------|---------|
| Docker Desktop | ≥ 4.20 |
| Docker Compose | ≥ 2.20 |
| RAM | ≥ 8 GB (16 GB recommended with Iceberg) |
| Disk | ≥ 5 GB |

---

## Step 1 — Start the Stack

Run from the `demo/` directory:

```bash
cd demo
docker compose up
```

This starts:
- **Engine** — Graph Query Engine REST API on `http://localhost:7000`
- **Jupyter** — Notebook server on `http://localhost:8888`
- **Trino** — SQL engine for Iceberg on `http://localhost:8080`
- **MinIO** — S3-compatible object store on `http://localhost:9000`
- **Iceberg REST** — Iceberg catalog on `http://localhost:8181`

> **H2-only (faster start, no Iceberg):**
> ```bash
> docker compose up engine jupyter
> ```

Wait until you see `[entrypoint] Setup complete` in the engine logs, then open Jupyter.

---

## Step 2 — Open Jupyter

Open **http://localhost:8888** in your browser.

**Password:** `GqeDemo123`

You will land directly in the `demo/` directory.

---

## Step 3 — AML (Anti-Money Laundering) Demo

**Notebook:** `aml/notebooks/aml_sql_showcase.ipynb`

This notebook demonstrates graph queries over a real financial transaction dataset (IBM AML dataset).

### How to run

1. Open `aml/notebooks/aml_sql_showcase.ipynb`
2. In **Step 0**, choose your backend:
   - `BACKEND = 'h2'` — embedded H2 database, no extra services needed
   - `BACKEND = 'iceberg'` — Trino + MinIO (full stack must be running)
3. Set `MAX_ROWS` (default `1_000_000`) and `WIPE = True` for a fresh load
4. **Run All Cells** — the notebook will:
   - Download the IBM AML dataset from Kaggle (first run only, ~30 MB)
   - Seed the database
   - Run all Gremlin queries automatically

> **Kaggle download:** Set your credentials before starting Docker:
> ```bash
> export KAGGLE_USERNAME=your_username
> export KAGGLE_KEY=your_api_key
> docker compose up
> ```
> Or place `data/aml-demo.csv` in the repo root `data/` directory to skip the download.

### What is demonstrated

| Section | Queries |
|---------|---------|
| 1 — Root steps | `g.V()`, `g.E()`, vertex by ID |
| 2 — Filters | `has`, `hasLabel`, `hasNot`, `is(pred)` |
| 3 — Hop steps | `out`, `in`, `both`, `outE`, `inE` |
| 4 — Path steps | `repeat().times(n)`, `simplePath()`, `path().by()` |
| 5 — Aggregations | `count`, `sum`, `mean`, `groupCount`, `dedup` |
| 6 — Projections | `project().by()`, `valueMap`, `choose()` (CASE WHEN) |
| 7 — Ordering | `order().by()`, `limit`, `dedup` |
| 8–9 — Where clauses | `where(outE.has(...))`, `where(and(...))` |
| 10–11 — Graph hops | Bank → Country traversals, account lookups |
| 12–15 — Multi-hop trails | 2-hop, 3-hop, 5-hop, 10-hop money laundering chains |

---

## Step 4 — Social Networking Demo

**Notebook:** `social_networking/social_network_demo.ipynb`

This notebook demonstrates graph queries over a small social network (10 people, 5 companies, 10 cities, 8 skills).

### How to run

1. Open `social_networking/social_network_demo.ipynb`
2. In **Step 0**, choose your backend:
   - `BACKEND = 'h2'` — embedded H2 (default)
   - `BACKEND = 'iceberg'` — Trino + MinIO (full stack must be running)
3. Set `WIPE = True` to reload data from scratch
4. **Run Step 1** — uploads the mapping and seeds the sample graph
5. **Run remaining cells** — executes all queries

### What is demonstrated

| Query | Description |
|-------|-------------|
| Q1 — All people | List all Person vertices |
| Q2 — All companies | List all Company vertices |
| Q3 — Connections | `KNOWS` relationships between people |
| Q4 — Employment | `WORKS_AT` relationships |
| Q5 — Location | `LIVES_IN` — people and their cities |
| Q6 — Skills | `HAS_SKILL` — people and their skills |
| Q7 — Friends of friends | 2-hop `KNOWS` traversal |
| Q8 — Colleagues | People who work at the same company |
| Q9 — Skill filter | People who know Python |
| Q10 — Company cities | `COMPANY_CITY` — where companies are headquartered |
| Q11 — City filter | Large cities (population > 3 million) |
| Q12 — Skill count | Number of skills per person |
| Q13 — Companies by industry | Filter companies by industry |

### Graph visualisation

Open `social_networking/full_graph_view.ipynb` to see a visual representation of the social network graph.

---

## Service Overview

| Service | URL | Description |
|---------|-----|-------------|
| Engine API | http://localhost:7000 | Gremlin REST endpoint |
| Jupyter | http://localhost:8888 | Notebooks (password: `GqeDemo123`) |
| Trino | http://localhost:8080 | SQL engine for Iceberg |
| MinIO API | http://localhost:9000 | S3-compatible object store |
| MinIO Console | http://localhost:9001 | MinIO web UI |
| Iceberg REST | http://localhost:8181 | Iceberg catalog |

---

## Stopping

```bash
# Stop containers (data volumes preserved)
docker compose down

# Stop and wipe all data volumes
docker compose down -v
```

---

## Troubleshooting

**Engine doesn't start:**
```bash
docker logs gqe-engine
```

**Jupyter can't reach engine:**  
Inside Jupyter, `BASE_URL` is `http://engine:7000` — not `localhost:7000`. This is set automatically.

**Trino not ready yet:**  
Trino can take 30–60 seconds to start. Check: `curl http://localhost:8080/v1/info`

**Seed fails with Trino connection error:**  
Ensure the full stack is running (`docker compose up`, not `docker compose up engine jupyter`).

**Building from source (after code changes):**
```bash
cd demo
docker compose up --build
```
