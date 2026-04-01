# Graph Query Engine

Accepts a graph mapping file and Gremlin traversal strings, translates supported traversals into SQL `SELECT` statements, and executes them through JDBC. Also supports native Gremlin execution via TinkerPop/TinkerGraph.

## Quick start

```bash
mvn exec:java   # start service on port 7000
mvn test                   # run all tests
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `7000` | HTTP port |
| `DB_URL` | `jdbc:h2:mem:graph;DB_CLOSE_DELAY=-1` | JDBC connection URL |
| `DB_USER` | `sa` | Database user |
| `DB_PASSWORD` | _(empty)_ | Database password |
| `DB_DRIVER` | `org.h2.Driver` | JDBC driver class |
| `GRAPH_PROVIDER` | `tinkergraph` | Gremlin execution backend |

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Service health |
| `POST` | `/gremlin/query` | Execute Gremlin natively (TinkerGraph) |
| `POST` | `/gremlin/query/tx` | Execute with transaction semantics |
| `GET` | `/gremlin/provider` | Active provider info |
| `POST` | `/query/explain` | SQL translation only (no execution) |
| `POST` | `/mapping/upload` | Upload JSON mapping file |
| `GET` | `/mapping/status` | Mapping store status |
| `GET` | `/mappings` | List stored mappings |
| `POST` | `/mapping/active` | Set active mapping (`?id=<id>`) |
| `POST` | `/admin/seed-demo` | Seed demo data |
| `POST` | `/admin/seed-10hop` | Seed 10-hop chain (SQL demo) |
| `POST` | `/admin/seed-gremlin-10hop-tx` | Seed 10-hop chain (TinkerGraph) |
| `POST` | `/admin/load-aml-csv` | Load AML CSV (`?path=…&maxRows=…`) |

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

Sample mappings in `mappings/`.

## Supported Gremlin steps (SQL-translatable)

`g.V()` · `g.E()` · `g.V(id)` · `.hasLabel()` · `.has(k,v)` · `.out()` · `.in()` · `.both()` · `.outE()` · `.inE()` · `.outV()` · `.inV()` · `.repeat(…).times(n)` · `.values()` · `.limit()` · `.count()` · `.project(…).by(…)` · `.groupCount().by()` · `.order().by()` · `.as()` · `.select()` · `.where()` · `.dedup()` · `.path().by()` · `.simplePath()`

Full TinkerPop semantics (including `cyclicPath`, `match`, etc.) are available via `/gremlin/query` (native execution).

## SQL explain mode

```bash
# Seed + upload mapping, then explain any traversal
curl -X POST http://localhost:7000/admin/seed-10hop
curl -X POST http://localhost:7000/mapping/upload \
  -F "file=@mappings/ten-hop-mapping.json"

curl -X POST http://localhost:7000/query/explain \
  -H "Content-Type: application/json" \
  -d '{"gremlin":"g.V(1).hasLabel(\"Node\").repeat(out(\"LINK\")).times(10)"}'
```

## AML demo (Jupyter notebook)

```bash
mvn exec:java                  # terminal 1
python3 scripts/normalize_aml.py          # first time only
jupyter notebook aml_demo_queries.ipynb   # terminal 2
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

## Project layout

```
src/main/java/com/graphqueryengine/
├── App.java                          # HTTP routes
├── gremlin/
│   ├── GremlinExecutionService.java
│   └── provider/
│       ├── GraphProvider.java        # Provider SPI
│       ├── TinkerGraphProvider.java
│       └── TinkerGraphTransactionApi.java
├── query/
│   ├── GremlinSqlTranslator.java     # Gremlin → SQL
│   └── QueryExecutionService.java
├── mapping/
├── db/
└── config/

mappings/       Sample mapping JSON files
benchmarks/     LSQB workload definitions
scripts/        normalize_aml.py, benchmark_lsqb.py
demo/data/      AML dataset CSVs
```

## IntelliJ sync

```bash
chmod +x scripts/intellij-reset.sh && ./scripts/intellij-reset.sh
```

Or: delete `GraphQueryEngine.iml`, run `mvn -U clean test`, then **Reload All Maven Projects**.
