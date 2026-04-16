# Graph Query Engine

Accepts a graph mapping file and Gremlin traversal strings, translates supported traversals into SQL `SELECT` statements, and executes them through JDBC. SQL is always the execution backend; WCOJ acceleration is built in and opt-out.

## Quick start

### Option 1 — Docker (recommended, zero setup)

```bash
cd demo
docker compose up          # full stack: engine + Iceberg/Trino + Jupyter
# OR — H2 only (lighter, no Iceberg):
docker compose up engine jupyter
```

- Notebooks: **http://localhost:8888** (password: `GqeDemo123`)
- Engine API: **http://localhost:7000**

Open the AML or Social Networking notebook and run cells top-to-bottom to load data and execute queries.

> **Kaggle data** (AML dataset) requires a Kaggle API token.  
> Set `KAGGLE_USERNAME` and `KAGGLE_KEY` environment variables before `docker compose up`,  
> or pass them via a `.env` file in the `demo/` directory.

### Option 2 — Local (Maven)

```bash
mvn exec:java   # start service on port 7000
mvn test        # run all tests
```

Then open a notebook:

```bash
jupyter notebook demo/aml/notebooks/aml_sql_showcase.ipynb
```

## Configuration

| Variable | Default                                      | Description |
|----------|----------------------------------------------|-------------|
| `PORT` | `7000`                                       | HTTP port |
| `DB_URL` | `jdbc:h2:file:./data/graph;AUTO_SERVER=TRUE` | JDBC connection URL for SQL translation/execution state (persistent file-based H2) |
| `DB_USER` | `sa`                                         | Database user |
| `DB_PASSWORD` | _(empty)_                                    | Database password |
| `DB_DRIVER` | `org.h2.Driver`                              | JDBC driver class |
| `GRAPH_PROVIDER` | `sql`                                        | Always `sql`. WCOJ is an embedded optimiser; use `WCOJ_ENABLED=false` to disable it. |
| `WCOJ_ENABLED` | `true`                                       | Enable/disable WCOJ Leapfrog optimiser (set `false` for pure SQL) |
| `WCOJ_MAX_EDGES` | `5000000`                                    | Max edges per table kept in the WCOJ in-memory index before spilling to disk |
| `WCOJ_DISK_QUOTA_MB` | `2048`                                       | Per-mapping disk quota for CSR index files (MB) |
| `WCOJ_INDEX_TTL_SECONDS` | `30`                                         | Seconds before a cached WCOJ index is rebuilt from source (0 = disable TTL) |

For a temporary in-memory SQL DB, set `DB_URL=jdbc:h2:mem:graph;DB_CLOSE_DELAY=-1`.

Run with explicit persistence paths:

```bash
DB_URL="jdbc:h2:file:./data/graph;AUTO_SERVER=TRUE" mvn exec:java
```

## Endpoints

| Method | Path | Description                                       |
|--------|------|---------------------------------------------------|
| `GET` | `/health` | Service health                                    |
| `POST` | `/gremlin/query` | Execute Gremlin natively (sql backend by default) |
| `POST` | `/query/explain` | SQL translation only (no execution)               |
| `POST` | `/mapping/upload` | Upload JSON mapping file                          |
| `GET` | `/mapping/status` | Mapping store status                              |
| `GET` | `/mappings` | List stored mappings                              |
| `POST` | `/mapping/active` | Set active mapping (`?id=<id>`)                   |

## Mapping file format

```json
{
  "vertices": {
    "Person": {
      "table": "people",
      "idColumn": "id",
      "properties": { "name": "name", "age": "age" }
    }
  },
  "edges": {
    "KNOWS": {
      "table": "knows",
      "idColumn": "id",
      "outColumn": "out_id",
      "inColumn": "in_id",
      "properties": { "since": "since_year" }
    }
  }
}
```

Iceberg table references are supported in `table` using the `iceberg:` prefix:
- Location/path targets (for example `s3://...`) are translated to DuckDB-compatible `iceberg_scan(...)`.
- Catalog identifiers (for example `prod.aml.accounts`) are emitted directly for engines like Trino/Spark.

```json
{
  "vertices": {
    "Account": {
      "table": "iceberg:s3://warehouse/aml/accounts",
      "idColumn": "id",
      "properties": { "accountId": "account_id" }
    }
  }
}
```

This translates to SQL like:

```sql
-- noinspection SqlNoDataSourceInspectionForFile

SELECT account_id AS accountId
FROM iceberg_scan('s3://warehouse/aml/accounts')
```

Catalog identifier example:

```json
{
  "vertices": {
    "Account": {
      "table": "iceberg:prod.aml.accounts",
      "idColumn": "id",
      "properties": { "accountId": "account_id" }
    }
  }
}
```

Which translates to:

```sql
-- noinspection SqlNoDataSourceInspectionForFile

SELECT account_id AS accountId
FROM prod.aml.accounts
```

Sample mappings in `mappings/`.
For Iceberg examples, see `mappings/iceberg-mapping.json`.
For local container testing with Trino, use `mappings/iceberg-local-mapping.json`.
For a full mapping reference (including `outColumn`/`inColumn` and FK-style relationships), see `MAPPINGS-README.md`.

## Local Iceberg container test

Start the full Iceberg stack (Trino + MinIO + Iceberg REST) using Docker Compose:

```bash
cd demo
docker compose up minio minio-init iceberg-rest trino
```

Or bring up just the lightweight Iceberg infra stack:

```bash
bash demo/infra/scripts/iceberg_local_up.sh
```

Start the engine in another terminal:

```bash
mvn exec:java
```

Upload the local Iceberg mapping and run a query:

```bash
curl -X POST http://localhost:7000/mapping/upload \
  -F "file=@demo/aml/mappings/iceberg-local-mapping.json" \
  -F "id=iceberg-local" \
  -F "activate=true"

curl -X POST http://localhost:7000/query/explain \
  -H "Content-Type: application/json" \
  -H "X-Mapping-Id: iceberg-local" \
  -d '{"gremlin":"g.V().hasLabel(\"Account\").values(\"accountId\").limit(5)"}'
```

To execute translated SQL directly in Trino:

```bash
docker exec -i iceberg-trino trino --server http://localhost:8080 \
  --execute "SELECT account_id FROM iceberg.aml.accounts LIMIT 5"
```

Tear down:

```bash
bash demo/infra/scripts/iceberg_local_down.sh
```

For stack internals and troubleshooting, see `infra/iceberg/README.md`.

## Supported Gremlin steps (SQL-translatable)

`g.V()` · `g.E()` · `g.V(id)` · `.hasLabel()` · `.has(k,v)` · `.out()` · `.in()` · `.both()` · `.outE()` · `.inE()` · `.outV()` · `.inV()` · `.repeat(…).times(n)` · `.values()` · `.limit()` · `.count()` · `.project(…).by(…)` · `.groupCount().by()` · `.order().by()` · `.as()` · `.select()` · `.where()` · `.dedup()` · `.path().by()` · `.simplePath()`

Full TinkerPop semantics (including `cyclicPath`, `match`, etc.) are available via `/gremlin/query` (native execution).

## SQL explain mode

```bash
# Upload mapping, then explain any traversal
curl -X POST http://localhost:7000/mapping/upload \
  -F "file=@mappings/ten-hop-mapping.json"

curl -X POST http://localhost:7000/query/explain \
  -H "Content-Type: application/json" \
  -d '{"gremlin":"g.V(1).hasLabel(\"Node\").repeat(out(\"LINK\")).times(10)"}'
```

For AML/Iceberg demo data setup, use scripts in `scripts/` and notebooks under the repository root.

```bash
# Iceberg mapping example (uses mappings/iceberg-mapping.json)
curl -X POST http://localhost:7000/mapping/upload \
  -F "file=@mappings/iceberg-mapping.json" \
  -F "id=iceberg-demo" \
  -F "activate=true"

curl -X POST http://localhost:7000/query/explain \
  -H "Content-Type: application/json" \
  -H "X-Mapping-Id: iceberg-demo" \
  -d '{"gremlin":"g.V().hasLabel(\"Account\").project(\"accountId\",\"category\").by(\"accountId\").by(choose(values(\"riskScore\").is(gt(0.7)),constant(\"WHALE\"),constant(\"RETAIL\"))).limit(5)"}'
```

## AML demo (Jupyter notebook)

### Docker (easiest)

```bash
cd demo
KAGGLE_USERNAME=<your-username> KAGGLE_KEY=<your-key> docker compose up
```

Open **http://localhost:8888** (password: `GqeDemo123`) and run `aml/notebooks/aml_sql_showcase.ipynb`.

### Local

The IBM AML dataset is **not checked into git** (files range from 32 MB to 16 GB).

```bash
# Requires Kaggle CLI and ~/.kaggle/kaggle.json API token
# Get your token at https://www.kaggle.com/settings → API → Create New Token
pip install kaggle
```

Then from the notebook (cell 1) set `MAX_ROWS` and `BACKEND` and run all cells — the notebook downloads and seeds data automatically.

Or manually:

```bash
mvn exec:java                                                         # terminal 1
jupyter notebook demo/aml/notebooks/aml_sql_showcase.ipynb           # terminal 2
```

Run cells top-to-bottom. Queries S1–S8 (simple) and C1–C11 (complex, including 10-hop) execute against the live service. See `QUICK_REFERENCE.md` for all queries in copy-paste form.

## Benchmarking

```bash
python3 scripts/benchmark_lsqb.py \
  --base-url http://localhost:7000 \
  --workload benchmarks/lsqb_aml_workload.json \
  --warmup 20 --iterations 200 --clients 4 \
  --mapping-id aml \
  --output-json output/bench-summary.json \
  --output-csv output/bench-requests.csv
```

See `BENCHMARKING.md` for full options and workload file format.

## Testing

### Unit Tests

Run all tests:
```bash
mvn test
```

## WCOJ — Worst-Case Optimal Join Engine

For multi-hop path queries (e.g. 3-hop money trails in AML), standard SQL joins suffer
from **intermediate result explosion** — joining Account→Transfer→Account for a 3-hop
chain produces O(E²) intermediate rows before the final filter is applied.

The WCOJ optimiser is **embedded inside the `sql` provider** and enabled by default.
It replaces binary SQL joins with a
[Leapfrog Trie Join](https://arxiv.org/abs/1210.0481) algorithm that:

- Loads edge tables into a **Compressed Sparse Row (CSR) in-memory index** on first use
- Uses **sorted `long[]` neighbour arrays** with O(log n) `seek()` for intersection
- Enumerates paths via **factorized depth-first recursion** — only one `long[]` per complete path is ever allocated
- Applies `simplePath()` cycle-exclusion as an O(depth) stack check (no HashSet)
- **Automatically falls back to SQL** for aggregations, projections, ORDER BY, edge queries, and tables exceeding `WCOJ_MAX_EDGES`

### Enable / disable WCOJ

```bash
# Default — WCOJ on, TTL=30 s
mvn exec:java

# Disable WCOJ entirely (pure SQL)
WCOJ_ENABLED=false mvn exec:java

# Force disk index for large edge tables
WCOJ_MAX_EDGES=500000 mvn exec:java
```

### Performance comparison (5-hop chain, 80k accounts)

| Approach | Intermediate rows | Complexity |
|---|---|---|
| SQL nested joins | Up to E⁴ ≈ billions | O(E^k) |
| CTE / subquery | E³ pruned late | O(E^(k-1)) |
| **WCOJ Leapfrog** | **≤ final result size** | **O(N log N)** |

### When WCOJ is used vs. SQL fallback

| Query shape | WCOJ | SQL |
|---|---|---|
| `.out()` / `.in()` / `.both()` hop traversals | ✅ | |
| `.repeat().times(n)` path queries | ✅ | |
| `.simplePath()` cycle exclusion | ✅ | |
| `.count()` after multi-hop | ✅ | |
| `.groupCount()`, `.order().by()` | | ✅ |
| `.project().by()` projections | | ✅ |
| `g.E()` edge-root queries | | ✅ |
| Tables > `WCOJ_MAX_EDGES` (5M rows) | | ✅ |

## Project layout

```
demo/
├── docker-compose.yml              # Full stack: engine + Iceberg + Jupyter
├── docker-mappings/                # Docker-specific mappings (trino:8080 URLs)
├── aml/                            # AML use-case notebooks, scripts, mappings
├── social_networking/              # Social-network use-case notebooks, scripts, mappings
├── infra/                          # Iceberg/Trino/MinIO infra scripts
└── requirements.txt                # Python dependencies for notebooks
docker/
├── Dockerfile.local                # Local build from pre-built JAR
└── entrypoint.sh                   # Engine startup + mapping upload
src/main/java/com/graphqueryengine/
├── App.java                        # HTTP routes
├── engine/wcoj/                    # WCOJ Leapfrog optimiser
├── gremlin/provider/               # SQL graph provider
├── query/                          # Gremlin → SQL translator
├── mapping/                        # Mapping store
├── db/                             # JDBC backend registry
└── config/
benchmarks/    LSQB workload definitions
```
