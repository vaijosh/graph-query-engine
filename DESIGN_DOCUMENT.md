# Graph Query Engine - Design Document

## 1) Purpose and Scope

The Graph Query Engine provides two complementary capabilities:

1. **Native Gremlin execution** via a graph provider (`/gremlin/query`, `/gremlin/query/tx`)
2. **Gremlin-to-SQL translation** for explain/reference (`/query/explain`)

The service is designed for a mapping-driven model where vertex/edge labels are mapped to relational tables and columns.

This document describes the current implementation in `src/main/java/com/graphqueryengine` and the current behavior covered by tests.

---
## 2) Top-Level Architecture

---
### Architecture Diagram

```text
┌───────────────────────────────────┐
│        GREMLIN REST CLIENT        │
│  (Notebooks / curl / demo_app.py) │
└────────────────┬──────────────────┘
                 │  HTTP  (port 7000)
                 ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                         REST SERVER — App.java  (Javalin)                              │
│                                                                                        │
│  ┌────────────────┐  ┌────────────────────────┐  ┌───────────────────────────────────┐ │
│  │ GET /health    │  │  Mapping Endpoints     │  │  Gremlin / Query Endpoints        │ │
│  │                │  │  POST /mapping/upload  │  │  POST /gremlin/query   (execute)  │ │
│  │                │  │  GET  /mapping         │  │  POST /gremlin/query/tx  (tx)     │ │
│  │                │  │  GET  /mappings        │  │  GET  /gremlin/provider           │ │
│  │                │  │  POST /mapping/active  │  │  POST /query/explain  (SQL)       │ │
│  │                │  │  DELETE /mapping       │  │  POST /query  ─► 307 redirect     │ │
│  │                │  │  GET  /mapping/status  │  │                                   │ │
│  └────────────────┘  └───────────┬────────────┘  └──────────────────┬────────────────┘ │
└──────────────────────────────────┼──────────────────────────────────┼─────────────────-┘
                                   │                                  │
               ┌───────────────────┘                                  │
               ▼                                                      ▼
┌──────────────────────────────┐       ┌────────────────────────────────────────────────────┐
│      MAPPING SUBSYSTEM       │       │              TRANSLATION PIPELINE                  │
│                              │       │                                                    │
│  ┌──────────────────────┐    │       │  ┌─────────────────────────────────────────────┐   │
│  │   MappingStore       │    │       │  │  GraphQueryTranslatorFactory (interface)    │   │
│  │  (in-memory +        │    │       │  │    DefaultGraphQueryTranslatorFactory       │   │
│  │   file-persisted)    │    │       │  │    env: QUERY_TRANSLATOR_BACKEND            │   │
│  │  StoredMapping       │    │       │  │         QUERY_PARSER                        │   │
│  │  activeMappingId     │    │       │  └──────────────────┬──────────────────────────┘   │
│  └──────────┬───────────┘    │       │                     │ creates                      │
│             │                │       │                     ▼                              │
│  ┌──────────▼───────────┐    │       │  ┌─────────────────────────────────────────────┐   │
│  │   MappingConfig      │◄───┼───────┤  │  GraphQueryTranslator (interface)           │   │
│  │   (vertices, edges)  │    │       │  │  ┌─────────────────────┐ ┌─────────────────┐│   │
│  └──────────────────────┘    │       │  │  │StandardSqlGraph     │ │IcebergSqlGraph  ││   │
│                              │       │  │  │QueryTranslator      │ │QueryTranslator  ││   │
│  ┌──────────────────────┐    │       │  │  │(STANDARD mode)      │ │(ICEBERG mode)   ││   │
│  │   VertexMapping      │    │       │  └──┤─────────────────────┴─┴─────────────────┤│   │
│  │   table, idColumn    │    │       │     │  SqlGraphQueryTranslator (base class)   │    │
│  │   properties{}       │    │       │     │  ┌───────────────────────────────────┐  │    │
│  └──────────────────────┘    │       │     │  │  GremlinSqlTranslator (facade)    │  │    │
│                              │       │     │  │  • dialect: Standard / Iceberg    │  │    │
│  ┌──────────────────────┐    │       │     │  │  • stepParser: GremlinStepParser  │  │    │
│  │   EdgeMapping        │    │       │     │  └───────────────────────────────────┘  │    │
│  │   table, out/inCol   │    │       │     └─────────────────────────────────────────┘    │
│  │   out/inVertexLabel  │    │       │                                                    │
│  └──────────────────────┘    │       └────────────────────────────────────────────────────┘   
│                              │                                                            
│  ┌──────────────────────┐    │                                                            
│  │  TableReference      │    │                                                            
│  │  Resolver            │    │                                                            
│  │  (iceberg: prefix,   │    │                                                            
│  │   DuckDB scan, etc.) │    │                                                            
└──┴──────────────────────┴────┘






┌────────────────────────────────────────────────────────────────────────────────────────┐
│              TRANSLATION PIPELINE — INTERNAL STAGES                                    │
│                                                                                        │
│  Gremlin String                                                                        │
│       │                                                                                │
│       ▼  STAGE 1: PARSING                                                              │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐  │
│  │  GremlinTraversalParser (interface)                                              │  │
│  │  ┌─────────────────────────────────┐  ┌─────────────────────────────────────┐    │  │
│  │  │  AntlrGremlinTraversalParser    │  │  LegacyGremlinTraversalParser       │    │  │
│  │  │  (Gremlin.g4 grammar)           │  │  (hand-written tokenizer)           │    │  │
│  │  └─────────────────────────────────┘  └─────────────────────────────────────┘    │  │
│  │                    │  GremlinParseResult { vertexQuery, rootIdFilter, steps }    │  │
│  └────────────────────┼─────────────────────────────────────────────────────────────┘  │
│                       │                                                                │
│       ▼  STAGE 2: STEP PARSING                                                         │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐  │
│  │  GremlinStepParser  →  ParsedTraversal                                           │  │
│  │  { label, filters, hops, limit, whereClause, projections,                        │  │
│  │    pathByProperties, asAliases, selectFields, aggregations }                     │  │
│  └────────────────────┬─────────────────────────────────────────────────────────────┘  │
│                       │                                                                │
│       ▼  STAGE 3: MAPPING RESOLUTION                                                   │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐  │
│  │  SqlMappingResolver  (label / property  →  table / column)                       │  │
│  └───────┬──────────────────┬────────────────────┬──────────────────────────────────┘  │
│          │                  │                    │                                     │
│       ▼  STAGE 4a: SQL RENDERING          ▼  STAGE 4b: QUERY PLAN (optional)           │
│  ┌───────────────────────┐         ┌──────────────────────────────────────────────┐    │
│  │  VertexSqlBuilder     │ g.V()   │  QueryPlanBuilder  →  QueryPlan              │    │
│  │  HopSqlBuilder        │ hops    │  { rootType, rootLabel, rootTable, dialect,  │    │
│  │  EdgeSqlBuilder       │ g.E()   │    filters, hops, aggregation, projections,  │    │
│  │  WhereClauseBuilder   │ WHERE   │    whereClause, limit … }                    │    │
│  │  SqlRenderHelper      │ utils   └──────────────────────────────────────────────┘    │
│  └───────────────────────┘                                                             │
│          │                                                                             │
│       SqlDialect (interface)                                                           │
│  ┌───────────────────────────────────────┐                                             │
│  │  StandardSqlDialect  STRING_AGG()     │                                             │
│  │  IcebergSqlDialect   ARRAY_JOIN(      │                                             │
│  │                        ARRAY_AGG())   │                                             │
│  └───────────────────────────────────────┘                                             │
│          │                                                                             │
│       Constants                                                                        │
│  ┌───────────────────────────────────────┐                                             │
│  │  GremlinToken  (step names, dirs)     │                                             │
│  │  SqlKeyword    (SELECT/FROM/JOIN/…)   │                                             │
│  └───────────────────────────────────────┘                                             │
│                                                                                        │
│  Output: TranslationResult { sql, parameters, plan? }                                  │
└────────────────────────────────────────────────────────────────────────────────────────┘



┌────────────────────────────────────────────────────────────────────────────────────────┐
│              GREMLIN EXECUTION ENGINE  (Native Graph Execution)                        │
│                                                                                        │
│  GremlinExecutionService                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│  │  GraphProvider (interface)     GraphProviderFactory  env: GRAPH_PROVIDER        │   │
│  │                                                                                 │   │
│  │  ┌──────────────────────────────────────────────────────────────────────────┐   │   │
│  │  │  WcojGraphProvider  (DEFAULT)                                            │   │   │
│  │  │  • AntlrGremlinTraversalParser + GremlinStepParser                       │   │   │
│  │  │  • isWcojEligible() → LeapfrogTrieJoin  (multi-hop paths)               │   │   │
│  │  │  • AdjacencyIndexRegistry  (two-tier: heap / disk-mapped CSR)            │   │   │
│  │  │  • SQL fallback  (via GremlinSqlTranslator + JDBC)                       │   │   │
│  │  └──────────────────────────────────────────────────────────────────────────┘   │   │
│  │                                                                                 │   │
│  │  ┌──────────────────────────────────────────────────────────────────────────┐   │   │
│  │  │  TinkerGraphProvider  (optional, set GRAPH_PROVIDER=tinkergraph)         │   │   │
│  │  │  • GremlinGroovyScriptEngine  (evaluates full TinkerPop traversals)      │   │   │
│  │  │  • TinkerGraph  (in-memory graph, loaded from ./data/graph.gryo)         │   │   │
│  │  └──────────────────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────────────────┘   │
│  Returns: GremlinExecutionResult { gremlin, results, resultCount }                     │
│           GremlinTransactionalExecutionResult { … + mode, status }                     │
└────────────────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────────────────┐
│              DATABASE ACCESS LAYER  (SQL Execution side)                               │
│                                                                                        │
│  DatabaseConfig  env: DB_URL · DB_USER · DB_PASSWORD · DB_DRIVER                       │
│  DatabaseManager  (JDBC DriverManager wrapper)  →  java.sql.Connection                 │
│                                                                                        │
│  ┌────────────────────────┐   ┌──────────────────────────────────────────────────┐     │
│  │  H2  (embedded, dev)   │   │  Trino + Iceberg Catalog  (production)           │     │
│  │  jdbc:h2:file:./data/  │   │  infra/iceberg/docker-compose.yml                │     │
│  └────────────────────────┘   └──────────────────────────────────────────────────┘     │
└────────────────────────────────────────────────────────────────────────────────────────┘

```

### Architecture difference with leading Zero ETL Graph engine
```Text

THIS ENGINE:
  Gremlin  →  GremlinStepParser  →  isWcojEligible()?
                  YES: LeapfrogTrieJoin (adjacency index)  →  property fetch via JDBC  →  result
                   NO: SQL text (GremlinSqlTranslator)  →  JDBC  →  [Trino]  →  Iceberg/S3
                                                                         ↑
                                                               separate process, must be running

PUPPYGRAPH:
  Gremlin  →  internal traversal plan  →  [embedded Spark]  →  Iceberg/Delta/Hudi  →  S3
                                                 ↑
                                       built-in, no separate SQL server


```


### Runtime Components

- **HTTP API layer**: `App` (Javalin routes, request parsing, error handling)
- **Mapping subsystem**: `MappingStore`, persisted mapping files, active mapping selection
- **Translation subsystem**:
  - parser abstraction: `GremlinTraversalParser`
  - parser implementations: legacy/manual and ANTLR
  - SQL compiler: `GremlinSqlTranslator`
  - mode wrappers: `StandardSqlGraphQueryTranslator`, `IcebergSqlGraphQueryTranslator`
- **Native execution subsystem**: `GremlinExecutionService` + `GraphProvider` SPI
  - **WCOJ engine** (default): `WcojGraphProvider` → `AdjacencyIndexRegistry` → `LeapfrogTrieJoin`
    - Two-tier index storage: in-memory `AdjacencyIndex` (heap) and `DiskBackedAdjacencyIndex` (memory-mapped CSR)
    - Per-mapping disk quota and isolation under `wcoj-index-cache/`
  - **TinkerGraph provider** (optional): full TinkerPop traversal via Groovy script engine
- **Database bootstrap**: `DatabaseConfig`, `DatabaseManager`

### High-Level Data Flow

```text
 SQL Translation path (POST /query/explain) ──────────────────────────────────────────
Client  →  App (Javalin)
           └─ resolveMappingForRequest()  →  MappingStore  →  MappingConfig
           └─ GraphQueryTranslator.translate(gremlin, mappingConfig)
                 └─ GremlinTraversalParser.parse()      →  GremlinParseResult
                 └─ GremlinStepParser.parse()           →  ParsedTraversal
                 └─ GremlinSqlTranslator.buildSql()
                       SqlMappingResolver  (label → table/column)
                       WhereClauseBuilder  (WHERE fragments)
                       VertexSqlBuilder / HopSqlBuilder / EdgeSqlBuilder
                       SqlRenderHelper + SqlDialect
                 └─ TranslationResult { sql, params, plan? }
           └─ QueryExplanation JSON  →  Client

── Native Gremlin execution path (POST /gremlin/query) ─────────────────────────────────
Client  →  App (Javalin)
           └─ GremlinExecutionService.execute(gremlin)
                 └─ WcojGraphProvider  (default provider)
                       ├─ AntlrGremlinTraversalParser.parse() + GremlinStepParser.parse()
                       │     → ParsedTraversal
                       ├─ isWcojEligible()?
                       │   YES:
                       │     AdjacencyIndexRegistry.getOrLoad()
                       │       ├─ rowCount ≤ 5M  →  AdjacencyIndex  (JVM heap HashMap)
                       │       └─ rowCount > 5M  →  DiskBackedAdjacencyIndex  (mmap CSR)
                       │     buildStartFilter()  →  seed IDs via SQL sub-query
                       │     LeapfrogTrieJoin.enumerate()
                       │       └─ depth-first DFS with seek() on sorted neighbour arrays
                       │     fetchProperty()  →  batched IN query for vertex properties
                       │   NO:
                       │     GremlinSqlTranslator.translate()  →  SQL
                       │     JDBC execute  →  ResultSet
                       └─ GremlinExecutionResult { gremlin, results, resultCount }
           └─ JSON  →  Client
```

---

## 3) API Surface (Current)

Implemented in `App`.

### Core Endpoints

- `GET /health`
- `POST /query/explain` - SQL translation only
- `POST /gremlin/query` - native execution
- `POST /gremlin/query/tx` - transactional execution mode wrapper
- `GET /gremlin/provider`

### Mapping Management

- `POST /mapping/upload`
- `GET /mapping`
- `GET /mappings`
- `GET /mapping/status`
- `POST /mapping/active?id=...`
- `DELETE /mapping?id=...`
---

## 4) Translator Selection and Modes

`DefaultGraphQueryTranslatorFactory` selects backend and parser from env vars:

- `QUERY_TRANSLATOR_BACKEND`:
  - `sql` / `legacy-sql` -> standard SQL mode
  - `iceberg` / `iceberg-sql` -> Iceberg SQL mode
- `QUERY_PARSER`:
  - `antlr`
  - `legacy` / `manual`

`SqlGraphQueryTranslator` contains mode behavior:

- `STANDARD` mode normally uses standard delegate
- If mappings look catalog-qualified (`table` contains `.`), standard mode falls back to Iceberg dialect renderer for compatibility
- `ICEBERG` mode uses Iceberg renderer directly

---

## 5) Parsing Pipeline

### Abstraction

- `GremlinTraversalParser` -> `GremlinParseResult` (normalized model)

### Parse Model

`GremlinParseResult` currently carries:

- `vertexQuery` (V/E root)
- `rootIdFilter` (e.g., `g.V(id)`)
- ordered `steps`

### Parsers

- **ANTLR parser**: grammar-backed parser path
- **Legacy parser**: manual parser retained for compatibility and tests

Both produce the same normalized model consumed by SQL translation.

---

## 6) SQL Translation Design

`GremlinSqlTranslator` is the main compiler.

### Responsibilities

- Parse step sequence into a richer internal traversal state (filters, hops, aggregates, projections, aliases, where clauses)
- Resolve mapping metadata for vertex/edge labels and properties
- Emit SQL text and positional parameters (`TranslationResult`)

### Coverage (Implemented)

The translator currently supports a substantial subset of traversal features including:

- root traversals (`g.V`, `g.E`, `g.V(id)`)
- filters (`hasLabel`, `has`, predicate forms like `gt/gte/...`)
- hops (`out`, `in`, `both`, `outE`, `inE`, `outV`, `inV`)
- aggregation (`count`, `sum`, `mean`, `groupCount`)
- projection (`project(...).by(...)`, neighbor fold forms)
- pathing (`repeat(...).times(n)`, `path().by(...)`, `simplePath()`)
- ordering, limits, dedup, aliases/select, selected `where(...)` forms

See **Section 17** for the full explicit capability matrix with SQL-mode support status for every recognized step.

### Notable Internal Strategies

- Correlated subqueries for many projection/where patterns
- Alias-based hop naming (`v0`, `e1`, `v1`, etc.)
- Branching for both-direction and multi-label traversal forms
- Defensive validation with explicit `IllegalArgumentException` messages for unsupported constructs

---

## 7) SQL Dialect Abstraction

### Interface

`SqlDialect` defines a small rendering surface:

- `quoteIdentifier(String)`
- `stringAgg(String expressionSql, String delimiter)`

### Standard Dialect

`StandardSqlDialect`:

- quoted identifiers with escaped `"`
- `STRING_AGG(expr, ',')`

### Iceberg Dialect

`IcebergSqlDialect`:

- inherits standard identifier quoting
- overrides string aggregation to:
  - `ARRAY_JOIN(ARRAY_AGG(expr), ',')`

This avoids environments where `STRING_AGG` is unavailable (e.g., targeted Trino runtime).

---

## 8) Mapping Model and Table Resolution

### Mapping Records

- `MappingConfig` with `vertices` and `edges`
- `VertexMapping`: `table`, `idColumn`, `properties`
- `EdgeMapping`: `table`, `idColumn`, `outColumn`, `inColumn`, `properties`, `outVertexLabel` *(optional)*, `inVertexLabel` *(optional)*

The optional endpoint labels explicitly declare which vertex label the `outColumn` and `inColumn` refer to.
This is the preferred form for schemas that use generic column names (`out_id` / `in_id`), where
column-stem heuristics cannot determine the correct vertex type.

```json
{
  "BELONGS_TO": {
    "table": "iceberg:aml.account_bank",
    "idColumn": "id",
    "outColumn": "out_id",
    "inColumn": "in_id",
    "outVertexLabel": "Account",
    "inVertexLabel": "Bank",
    "properties": {}
  }
}
```

Accepted aliases for JSON deserialization:

| Canonical field   | Accepted aliases          |
|-------------------|---------------------------|
| `outVertexLabel`  | `sourceLabel`, `outLabel` |
| `inVertexLabel`   | `targetLabel`, `inLabel`  |

Omitting these fields is safe — all existing mappings continue to work via heuristic resolution.

### Table Reference Resolution

`TableReferenceResolver` translates mapping table strings:

- plain table -> emitted as-is
- `iceberg:<location>` -> `iceberg_scan('<location>')`
- `iceberg:<catalog.schema.table>` -> emitted as `<catalog.schema.table>`

This allows both file-path and catalog-identifier Iceberg targets.

### Mapping Lifecycle

`MappingStore` provides:

- concurrent in-memory map
- active mapping pointer
- upload/update/list/select/delete
- persistence hooks via `App` (`.mapping-store` directory + active marker file)

---

## 9) Execution Model and Separation of Concerns

The service intentionally separates:

- **Translation** (`/query/explain`) from
- **Execution** (`/gremlin/query`)

`/query/explain` never executes SQL. It returns SQL + parameters for inspection.

Native Gremlin execution is routed through `GremlinExecutionService` and `GraphProvider` SPI.  The default provider is `WcojGraphProvider` which uses the Worst-Case Optimal Join algorithm for multi-hop path traversals and falls back to SQL for all other query shapes.

This keeps SQL generation deterministic and testable while preserving full Gremlin semantics in native mode.

---

## 9a) Worst-Case Optimal Join (WCOJ) Engine

### Motivation

Standard SQL binary joins process two tables at a time.  A 3-hop chain
`Account → Transfer → Account → Transfer → Account` can produce O(E²)
intermediate rows even when the final result set is tiny — the classic
**intermediate result explosion** problem.

The WCOJ engine solves this by replacing the SQL join pipeline with a
**Leapfrog Trie Join** (Veldhuizen, 2014) operating directly on sorted
in-memory or memory-mapped adjacency lists.  The result: intermediate state
never exceeds the theoretical worst-case bound of the final output.

---

### Component Overview

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         WCOJ ENGINE  (engine/wcoj/)                             │
│                                                                                 │
│  WcojGraphProvider  (GraphProvider SPI)                                         │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  1. Parse traversal (AntlrGremlinTraversalParser + GremlinStepParser)     │  │
│  │  2. isWcojEligible() ?                                                    │  │
│  │     YES ──► executeWcoj()         NO ──► executeSql() (SQL fallback)      │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                                                                 │
│  AdjacencyIndexRegistry  (two-tier index management)                            │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  rowCount ≤ 5 M?                                                          │  │
│  │    YES ──► AdjacencyIndex          (JVM heap HashMap<Long, long[]>)       │  │
│  │    NO  ──► DiskBackedAdjacencyIndex (memory-mapped CSR files)             │  │
│  │            within per-mapping disk quota (default 2 GB)?                  │  │
│  │              NO ──► return null → SQL fallback                            │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                                                                 │
│  LeapfrogTrieJoin  (core algorithm)                                             │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  indices[]: NeighbourLookup[]   one per hop                               │  │
│  │  outDirections[]: boolean[]     true=out, false=in                        │  │
│  │  simplePath: boolean            visited-set pruning                       │  │
│  │  startFilter: LongPredicate     seed vertex filter (from SQL sub-query)   │  │
│  │  limit: int                     early-termination                         │  │
│  │                                                                           │  │
│  │  enumerate() → List<long[]>  (each element is a complete path)           │  │
│  │    depth-first DFS with O(log n) seek() per step                         │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                                                                 │
│  LeapfrogIterator  (primitive sorted-array cursor)                              │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  next()          advance by 1                                             │  │
│  │  seek(key)       binary-search jump to smallest element ≥ key — O(log n) │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

### Algorithm: Leapfrog Trie Join

The join processes **all hop levels simultaneously** via depth-first recursion.
At each depth it holds exactly one `long` (the current vertex ID) and a sorted
neighbour array to iterate over.  No partial row is materialised until the full
path depth is reached.

```text
LEAPFROG DFS  (hop count = k)
─────────────────────────────────────────────────────────
enumerate():
  for each seed vertex v0 (from first index's outSources):
    if startFilter(v0) fails → skip
    path[0] = v0
    dfs(path, depth=0)

dfs(path, depth):
  if depth == k  → emit path[]  (single allocation)
  current = path[depth]
  neighbours[] = indices[depth].outNeighbours(current)     // O(1) HashMap or O(log V) disk CSR
  for each neighbour n in neighbours[]:
    if simplePath && n already in path[0..depth] → skip   // O(k) visited check, no allocation
    path[depth+1] = n
    dfs(path, depth+1)
─────────────────────────────────────────────────────────
Complexity:
  Time:   O(N · log N)   N = |output paths|   (vs O(Eᵏ) for nested-loop SQL)
  Memory: O(k + N)       k = hop depth        (vs O(E^(k-1)) for SQL join materialisation)
```

**Leapfrog intersection** is used for multi-label pruning and `both()` hops:

```text
leapfrogIntersect(a[], b[]):  O(m + n)  — no extra allocation
leapfrogUnion(a[], b[]):      O(m + n)  — used for both() direction expansion
```

---

### Two-Tier Index Storage

| Tier | Class | When used | Lookup | Notes |
|------|-------|-----------|--------|-------|
| **1 — JVM heap** | `AdjacencyIndex` | edge table ≤ 5 M rows | O(1) `HashMap<Long, long[]>` | Loaded lazily from JDBC on first access; cached until `invalidate()` |
| **2 — Disk CSR** | `DiskBackedAdjacencyIndex` | edge table > 5 M rows (within quota) | O(log V) binary search in memory-mapped file | Written once, reused across restarts; OS page cache keeps hot pages in RAM |
| **SQL fallback** | — | quota exceeded or ineligible query | via `GremlinSqlTranslator` | Transparent to the caller |

#### On-disk CSR File Layout (per direction: `.out.csr` / `.in.csr`)

```text
Offset    Size       Field
──────────────────────────────────────────────────────────
0         8 B        vertexCount   (number of distinct source vertices)
8         8 B        edgeCount     (total edges in this direction)
16        8 B        reserved
24        V × 16 B   Vertex table  [vertexId(8) | edgeOffset(8)]  — sorted by vertexId
24+V×16   E × 8 B    Edge array    [neighbourId(8)]  — sorted within each vertex's slice
──────────────────────────────────────────────────────────
Lookup: binary-search vertex table for vertexId → read slice from edge array
Thread-safe: each call uses ByteBuffer.duplicate() for an independent position cursor
```

---

### Per-Mapping Disk Quota & Multi-Tenant Isolation

`AdjacencyIndexRegistry` provides full isolation between concurrent client sessions
using different mappings:

```text
wcoj-index-cache/
  aml-iceberg/             ← mapping ID "aml-iceberg"
    aml_transfers_out_id_in_id.out.csr
    aml_transfers_out_id_in_id.in.csr
  social-h2/               ← mapping ID "social-h2"
    sn_knows_out_id_in_id.out.csr
    …
```

- Each mapping's disk usage is tracked independently with an `AtomicLong` counter.
- Default per-mapping quota: **2 GB** (configurable via `wcoj.disk.quota.mb` system property).
- If a new index build would exceed the quota, `getOrLoad()` returns `null` and the
  provider transparently falls back to SQL for that query.
- Mapping invalidation (`invalidateMapping(id)`) deletes all index files for that mapping
  and resets the quota counter.

---

### WCOJ Eligibility Check

`WcojGraphProvider.isWcojEligible()` determines whether a parsed traversal can be
accelerated.  WCOJ is used when **all** of the following hold:

| Condition | Reason |
|-----------|--------|
| `hops.size() ≥ 1` | No hops → no join to optimise; delegate to SQL |
| No `sum` / `mean` / `groupCount` | Aggregations require full SQL |
| No `project(…)` projections | Complex projections resolved by SQL |
| No `select(…)` fields | Alias-based projections require SQL context |
| No `orderBy` | Ordering over result set is a SQL concern |
| No `outE` / `inE` / `bothE` hops | Edge-step hops not yet supported in WCOJ |
| All hops are single-label | Multi-label hops are a pending TODO |

Everything else (path queries, `count()`, `dedup()`, `simplePath()`, `limit()`,
`has()` filters, `values()`) is handled natively by the WCOJ engine.

---

### Property Resolution after WCOJ

After the Leapfrog join produces a `List<long[]>` of vertex-ID paths, vertex
properties (e.g. `accountId`, `name`) are fetched with a **single batched SQL
query per hop position**:

```sql
SELECT id, account_id FROM aml_accounts WHERE id IN (?, ?, …)
```

This is O(|result-set|) rather than O(|path| × |table|) — the join itself never
touches property columns.

---

### SQL Fallback Path

For ineligible queries (aggregations, projections, ordering, edge-step hops) the
provider delegates transparently to `GremlinSqlTranslator` → JDBC, exactly as if
no WCOJ engine were present.  The SQL generated is identical to what `/query/explain`
returns, and is logged when `SQL_TRACE` / `X-SQL-Trace` is enabled.

---

### Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `wcoj.cache.dir` | `<cwd>/wcoj-index-cache` | Root directory for disk CSR files |
| `wcoj.disk.quota.mb` | `2048` | Per-mapping disk quota in MB |
| `AdjacencyIndexRegistry.DEFAULT_MAX_EDGES_IN_MEMORY` | `5,000,000` | Row threshold for heap vs disk tier |

---

## 10) Observability and Operational Behavior

### SQL Trace

- Global env: `SQL_TRACE`
- Per-request override: `X-SQL-Trace`

When enabled and a mapping exists, SQL translation logs are emitted for:

- `/query/explain`
- `/gremlin/query`
- `/gremlin/query/tx`

If a traversal is unsupported by SQL translator, native execution still proceeds; SQL trace logs note unavailability.

### Mapping Selection

- Per-request header: `X-Mapping-Id`
- Fallback to active mapping

---

## 11) Recent Hardening in Traversal Compilation

The current implementation includes fixes for multi-label repeat traversal behavior used by the Iceberg notebook C5 query.

### A) Sequential expansion for `repeat(out('A','B')).times(2)`

When label count matches repeat count, labels are expanded as ordered sequential single-label hops (A then B) rather than repeating the same multi-label hop object.

### B) Improved target vertex inference for generic edge columns (`in_id`/`out_id`)

Additional fallback logic infers target vertex from edge table tokens (e.g., `account_bank` -> `Bank`, `bank_country` -> `Country`) when column-stem heuristics are ambiguous.

Result: generated SQL now follows expected chain joins in C5-style traversals.

---

## 12) Testing Strategy and Coverage

### Current Test Layers

- Unit tests for translation behavior (`GremlinSqlTranslatorTest` variants)
- Notebook parity tests for end-to-end query sets
  - `NotebookQueryCoverageTest`
  - `IcebergNotebookQueryCoverageTest`
- Parser tests (ANTLR + legacy)
- Factory/selection tests (`DefaultGraphQueryTranslatorFactoryTest`)
- Gremlin compatibility tests

### Characteristics

- SQL text assertions for critical patterns
- Parameter list assertions
- Coverage of simple and complex AML notebook query sets (S1-S8, C1-C11)

---

## 13) Known Design Constraints

1. SQL translation supports a curated Gremlin subset, not full TinkerPop semantics.
2. Several advanced translator branches rely on heuristic mapping resolution in multi-vertex schemas.
3. Explain-mode SQL is reference output; execution semantics are provided by native Gremlin endpoints.
4. Mapping quality (labels, property coverage, edge direction columns) strongly influences translation fidelity.

---

## 14) Extensibility Points

1. **New parser modes**: implement `GremlinTraversalParser` and wire in factory
2. **New SQL backends/dialects**: implement `SqlDialect`; optionally add backend mode in factory
3. **Alternative graph execution backends**: implement `GraphProvider` and update provider factory
4. **Additional query translators**: implement `GraphQueryTranslator` and register via factory selection

---

## 15) Configuration Summary

Important env vars in current design:

- `PORT`
- `DB_URL`, `DB_USER`, `DB_PASSWORD`, `DB_DRIVER`
- `GRAPH_PROVIDER`
- `QUERY_TRANSLATOR_BACKEND`
- `QUERY_PARSER`
- `SQL_TRACE`
- `MAPPING_STORE_DIR`
- `MAPPING_PATH` (startup preload)

---

## 16) Suggested Next Design Evolutions

1. Introduce backend-specific SQL conformance tests (H2, Trino, DuckDB) from shared notebook workloads
2. Add docs for translator invariants (aliasing, parameterization, join ordering) to stabilize future refactors

---

## 17) SQL-Mode Capability Matrix

This section is the authoritative reference for which Gremlin steps are supported in SQL translation mode (`/query/explain`, `?plan=true`).
Native execution (`/gremlin/query`) supports the full TinkerPop step library regardless of this table.

### Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | Fully supported — translated to SQL |
| ⚠️ | Partially supported — works in specific forms only (see notes) |
| ❌ | Not supported in SQL mode — throws `IllegalArgumentException`; use `/gremlin/query` for native execution |

---

### Root steps

| Step | SQL support | Notes |
|------|-------------|-------|
| `g.V()` | ✅ | Requires `hasLabel` when multiple vertex labels are mapped |
| `g.E()` | ✅ | Requires `hasLabel` when multiple edge labels are mapped |
| `g.V(id)` | ✅ | Translated to `WHERE id_column = ?` |

---

### Filter steps

| Step | SQL support | Notes |
|------|-------------|-------|
| `.hasLabel(l)` | ✅ | Resolves the root table via mapping |
| `.has(k, v)` | ✅ | Equality filter; maps property name via mapping |
| `.has(k, gt(v))` / `gte` / `lt` / `lte` / `neq` / `eq` | ✅ | Predicate forms translated to `>`, `>=`, `<`, `<=`, `!=`, `=` |
| `.hasNot(k)` | ✅ | Translated to `IS NULL` |
| `.hasId(id)` | ✅ | Translated to `WHERE id_column = ?` |
| `.is(predicate)` | ⚠️ | Supported after `values('property')` and inside `where(select(…).is(gt/gte(n)))`. Example: `g.V().hasLabel('Person').values('age').is(gt(30)).limit(2)` → `SELECT age AS age FROM people WHERE age > ? LIMIT 2` |

---

### Traversal / hop steps

| Step | SQL support | Notes |
|------|-------------|-------|
| `.out(l)` | ✅ | Single or multiple labels; multi-label expands to UNION or LEFT JOINs |
| `.in(l)` | ✅ | Same as `out` with reversed join direction |
| `.both(l)` | ✅ | Expands to UNION of out-branch and in-branch |
| `.outE(l)` | ✅ | Edge hop; combines with `.inV()`/`.outV()` or used standalone in projections |
| `.inE(l)` | ✅ | Edge hop; combines with `.inV()`/`.outV()` or used standalone in projections |
| `.outV()` | ✅ | Resolved against prior `inE`; or as vertex traversal from `g.E()` |
| `.inV()` | ✅ | Resolved against prior `outE`; or as vertex traversal from `g.E()` |
| `.bothE(l)` | ✅ | Supported as hop step and in `where(...)` correlated subquery predicates |
| `.repeat(out/in/both(l)).times(n)` | ✅ | Expands to `n` JOIN hops; sequential label expansion when label count equals `n` |
| `.until(…)` | ❌ | Conditional termination not translatable to SQL |
| `.emit()` | ❌ | Not supported in SQL mode |
| `.loops()` | ❌ | Not supported in SQL mode |

---

### Aggregation steps

| Step | SQL support | Notes |
|------|-------------|-------|
| `.count()` | ✅ | `COUNT(*)` or `COUNT(DISTINCT id)` with `dedup()` |
| `.sum()` | ✅ | Requires preceding `values(p)`; emits `SUM(column)` |
| `.mean()` | ✅ | Requires preceding `values(p)`; emits `AVG(column)` |
| `.groupCount().by(p)` | ✅ | `SELECT col, COUNT(*) … GROUP BY col` |
| `.groupCount().by(select(a).by(p))` | ✅ | Alias-keyed group-count with hop joins |
| `.max()` / `.min()` | ❌ | Not supported in SQL mode |
| `.fold()` | ⚠️ | Only as terminal in `by(out(l).values(p).fold())` neighbor-list projections |
| `.unfold()` | ❌ | Not supported in SQL mode |

---

### Projection steps

| Step | SQL support | Notes |
|------|-------------|-------|
| `.values(p)` | ✅ | Projects a single property column |
| `.project(a…).by(p)` | ✅ | Multi-field SELECT; each `by(…)` maps to a named column |
| `.by(outV().values(p))` | ✅ | In `project`: fetches out-vertex property via JOIN |
| `.by(inV().values(p))` | ✅ | In `project`: fetches in-vertex property via JOIN |
| `.by(outE(l).count())` | ✅ | In `project`: correlated subquery for edge degree |
| `.by(inE(l).count())` | ✅ | In `project`: correlated subquery for edge degree |
| `.by(out(l).count())` / `in(l).count()` | ✅ | In `project`: correlated subquery counting connected vertices |
| `.by(out(l).values(p).fold())` | ✅ | In `project`: aggregated neighbor property list (`STRING_AGG` or `ARRAY_JOIN`) |
| `.by(choose(values(p).is(pred(v)), constant(a), constant(b)))` | ✅ | In `project`: `CASE WHEN … THEN … ELSE … END` |
| `.elementMap()` | ❌ | Not supported in SQL mode |
| `.valueMap()` | ✅ | Supported for root and hop traversals (mapping-backed property columns) |
| `.properties()` | ❌ | Not supported in SQL mode |

---

### Ordering and paging steps

| Step | SQL support | Notes |
|------|-------------|-------|
| `.order().by(p)` | ✅ | `ORDER BY mapped_column ASC` |
| `.order().by(p, Order.desc)` | ✅ | `ORDER BY mapped_column DESC` |
| `.order().by(select(a), Order.asc\|desc)` | ✅ | Order by a projected alias |
| `.order().by(values, desc)` | ✅ | Ordering a `groupCount` result by count descending |
| `.limit(n)` | ✅ | Pre-hop or post-hop depending on position in traversal |
| `.limit(scope, n)` | ⚠️ | Scope argument is accepted but ignored; treated as `limit(n)` |
| `.range(lo, hi)` | ❌ | Not supported in SQL mode |
| `.tail(n)` | ❌ | Not supported in SQL mode |
| `.skip(n)` | ❌ | Not supported in SQL mode |

---

### Path / cycle steps

| Step | SQL support | Notes |
|------|-------------|-------|
| `.path().by(p)` | ✅ | Emits per-hop property columns (`prop0`, `prop1`, …) |
| `.simplePath()` | ✅ | Adds `vN.id <> vM.id` / `NOT IN` WHERE conditions across hops |
| `.cyclicPath()` | ❌ | Not supported in SQL mode |

---

### Dedup / identity steps

| Step | SQL support | Notes |
|------|-------------|-------|
| `.dedup()` | ✅ | `SELECT DISTINCT …` or `COUNT(DISTINCT id)` |
| `.identity()` | ✅ | No-op traversal step; also supported in `project(...).by(identity())` |
| `.sideEffect(…)` | ❌ | Not supported in SQL mode |

---

### Alias / select / where steps

| Step | SQL support | Notes |
|------|-------------|-------|
| `.as(label)` | ✅ | Records hop index for later `select`/`where` reference |
| `.select(a…).by(p)` | ✅ | Projects aliased hop vertex columns |
| `.where(a, neq(b))` | ✅ | `vX.id <> vY.id` or property-level neq with `.by(p)` |
| `.where(eq(a))` | ✅ | `currentAlias.id = vX.id` |
| `.where(outE\|inE\|bothE(l).has(…))` | ✅ | Correlated `EXISTS` subquery |
| `.where(out\|in(l).has(…))` | ✅ | Correlated `EXISTS` with vertex JOIN |
| `.where(select(a).is(gt\|gte(n)))` | ✅ | `HAVING`-style outer filter on projected alias |
| `.where(and(…))` / `.where(or(…))` | ⚠️ | Supported for recursive composition of traversal-style `where` predicates (`outE/inE/bothE`, `out/in`) |
| `.where(not(…))` | ⚠️ | Supported as `NOT (...)` for traversal-style `where` predicates |
| `.not(…)` | ❌ | Standalone step form is not supported; use `where(not(...))` |
| `.match(…)` | ❌ | Not supported in SQL mode |

---

### Branch / conditional steps

| Step | SQL support | Notes |
|------|-------------|-------|
| `.choose(values(p).is(pred), constant(a), constant(b))` | ✅ | Only inside `project(…).by(choose(…))` |
| `.choose(…)` (general) | ❌ | Not supported in SQL mode outside `project.by` |
| `.coalesce(…)` | ❌ | Not supported in SQL mode |
| `.optional(…)` | ❌ | Not supported in SQL mode |
| `.union(…)` | ❌ | Not supported in SQL mode (internal UNIONs are generated for `both`/multi-label hops) |

---

### Unsupported / native-only steps

All other TinkerPop steps — including `addV`, `addE`, `drop`, `property`, `inject`, `constant` (standalone), `math`, `barrier`, `subgraph`, `profile`, `explain`, `pageRank`, `peerPressure`, `shortestPath` — are **not** translated to SQL.
These are available at full semantics via `/gremlin/query` (native TinkerGraph execution).

See **Section 18** for a detailed implementation-effort analysis of every unsupported step.

---

## 18) Implementation Effort for Unsupported SQL-Mode Steps

This section analyses the work required to bring each currently-unsupported (❌) or partially-supported (⚠️) step to full SQL translation.

### How to read this section

Every step is assessed against the three-layer pipeline:

- **Layer 1 (Grammar/Parser)** — `Gremlin.g4` already accepts any `IDENT(args)` chain, so changes are only needed when a step's arguments contain anonymous traversals (`__.as(…)`) or bare integer lists that the current `nestedCall` rule does not surface correctly.
- **Layer 2 (`parseSteps`)** — the `switch` in `GremlinSqlTranslator.parseSteps()` and the `ParsedTraversal` record fields that hold the new state.
- **Layer 3 (SQL builders)** — `buildVertexSql`, `buildEdgeSql`, `buildHopTraversalSql`, `buildVertexProjectionSql`, and/or new builder methods that emit the SQL.

**Complexity scale:** Low (< 1 day) · Medium (1–3 days) · High (3–7 days) · Very High / Incompatible (impractical without recursive SQL or application-side logic).

---

### Group A — Completing partially-supported steps (⚠️)

| Step | SQL equivalent | Layer 2 changes | Layer 3 changes | Complexity | Key risk |
|------|---------------|-----------------|-----------------|------------|----------|
| `.hasId(id)` | ✅ Implemented | Implemented in `parseSteps` and existing `id` filter emitters | N/A | **Done** | Covered by `translatesHasIdOnVertexRoot` |
| `.hasNot(k)` | ✅ Implemented | Implemented with `IS NULL` operator sentinel in filters | N/A | **Done** | Covered by `translatesHasNotAsIsNullPredicate` |
| `.is(pred)` | ⚠️ Partially implemented | Implemented for `values('p').is(pred(...))` and `where(select(...).is(...))` forms | N/A | **Partial** | Standalone/general `is(...)` forms outside value/select contexts remain unsupported |
| `.bothE(l)` as hop | ✅ Implemented | Implemented as a first-class hop step | N/A | **Done** | Covered by `translatesBothEAsHopStep` |
| `.limit(scope, n)` | `LIMIT n` (scope ignored) | Already accepted (2-arg form strips first arg) — behaviour is already correct | None | **Already done** | The scope argument (`Scope.local`) has no SQL analogue; document the limitation |

---

### Group B — Aggregation gaps

| Step | SQL equivalent | Layer 2 changes | Layer 3 changes | Complexity | Key risk |
|------|---------------|-----------------|-----------------|------------|----------|
| `.max()` | `SELECT MAX(col)` | Add `maxRequested` boolean to `ParsedTraversal`; set in `"max"` switch case | Mirror the `sum`/`mean` path in all builders — emit `MAX(mapped_col) AS max` | **Low** | Requires `values(p)` before `max()` just like `sum()`; enforce same validation |
| `.min()` | `SELECT MIN(col)` | Same as `max()` with `minRequested` flag | Emit `MIN(mapped_col) AS min` | **Low** | Same as `max()` |
| `.unfold()` | No direct SQL equivalent | `unfold()` de-nests an in-memory list — in SQL context it only makes sense after a `fold()` result, which itself only appears in `project.by` sub-expressions | Would need to materialise the fold subquery as a lateral join or VALUES row-set — dialect-specific and not portable | **Very High** | Not practically mappable to portable SQL; recommend native execution |

---

### Group C — Projection gaps

| Step | SQL equivalent | Layer 2 changes | Layer 3 changes | Complexity | Key risk |
|------|---------------|-----------------|-----------------|------------|----------|
| `.elementMap()` | `SELECT col1 AS 'prop1', col2 AS 'prop2', …` for all mapped properties | Add `elementMapRequested` boolean to `ParsedTraversal` | In builders: enumerate all `mapping.properties()` entries and emit `alias.col AS 'propName'` plus the id column; must handle vertex and edge cases separately | **Medium** | Output shape is a fixed-schema SQL row, not a true dynamic map; unmapped properties are silently omitted |
| `.valueMap()` | ✅ Implemented | Implemented with mapping-backed SELECT list generation | N/A | **Done** | Covered by `translatesValueMapOnVertexRoot` and hop/edge paths |
| `.properties()` | `SELECT col AS value, 'propName' AS key …` UNION per property | More complex — TinkerPop returns property _objects_; SQL would need one row per property | Requires UNION of per-column selects, one row per mapped property; impractical for multi-hop contexts | **High** | Very different result shape from all other steps; SQL result cannot carry TinkerPop `Property` semantics |

---

### Group D — Ordering and paging gaps

| Step | SQL equivalent | Layer 2 changes | Layer 3 changes | Complexity | Key risk |
|------|---------------|-----------------|-----------------|------------|----------|
| `.range(lo, hi)` | `LIMIT (hi-lo) OFFSET lo` | Add `rangeOffset`/`rangeLimit` fields to `ParsedTraversal`; parse 2-arg form of `range()` | Add `appendOffset(sql, offset)` helper in all builders; extend `SqlDialect` interface with `offsetClause(int)` since H2/Trino/DuckDB syntax differs | **Medium** | `OFFSET` syntax varies across dialects — standard SQL uses `OFFSET n ROWS`, H2/Trino use `OFFSET n` |
| `.skip(n)` | `OFFSET n` (no limit) | Add `skipOffset` to `ParsedTraversal`; set in `"skip"` case | Same `appendOffset` helper; emit only OFFSET with no LIMIT when `limit` is null | **Low** | Same dialect variation as `range()`; must not emit `LIMIT ALL` for dialects that require it |
| `.tail(n)` | Subquery: `SELECT * FROM (SELECT * FROM t ORDER BY id DESC LIMIT n) _t ORDER BY id ASC` | Add `tailRequested`/`tailLimit` to `ParsedTraversal` | Wrap the entire query in a reversed-order subquery then re-sort ascending; fragile when no stable sort key is defined | **High** | Requires a stable ordering column; without one, SQL `DESC LIMIT n` does not guarantee last-n semantics — needs an explicit tie-break column from the mapping |

---

### Group E — Path and cycle gaps

| Step | SQL equivalent | Layer 2 changes | Layer 3 changes | Complexity | Key risk |
|------|---------------|-----------------|-----------------|------------|----------|
| `.cyclicPath()` | `WHERE vN.id IN (v0.id, …, vN-1.id)` | Add `cyclicPathRequested` boolean; set in `"cyclicPath"` case; mutually exclusive with `simplePath` | In `buildHopTraversalSql` add a WHERE condition that is the logical negation of `simplePath` — current aliases already tracked | **Low** | Semantically `cyclicPath` is `NOT simplePath` — reuse the alias tracking already built for `simplePath`; must validate mutual exclusion |

---

### Group F — Identity and side-effect gaps

| Step | SQL equivalent | Layer 2 changes | Layer 3 changes | Complexity | Key risk |
|------|---------------|-----------------|-----------------|------------|----------|
| `.identity()` | No-op — pass through unchanged | Add `"identity"` case that does nothing (`// no-op`) | None | **Low** | Purely cosmetic step; no SQL change needed |
| `.barrier()` | No-op in SQL context — SQL is already set-oriented | Add `"barrier"` case that does nothing | None | **Low** | `barrier()` forces eager evaluation in TinkerPop lazy pipelines; SQL evaluation is already eager |
| `.sideEffect(…)` | Not mappable — executes a traversal for mutation side effects while passing through the stream | Would need to parse the inner traversal argument as a sub-traversal, translate it, and execute it separately | Cannot be expressed as a single SQL statement; would require two separate queries | **Very High** | Fundamentally a two-statement pattern; not expressible as a pure SQL SELECT |

---

### Group G — Compound WHERE and matching gaps

| Step | SQL equivalent | Layer 2 changes | Layer 3 changes | Complexity | Key risk |
|------|---------------|-----------------|-----------------|------------|----------|
| `.where(and(p1, p2, …))` | ✅ Implemented | Recursive `parseWhere()` support with predicate trees | N/A | **Done** | Covered by `translatesWhereAndPredicates` |
| `.where(or(p1, p2, …))` | ✅ Implemented | Recursive `parseWhere()` support with predicate trees | N/A | **Done** | Covered by `translatesWhereOrPredicates` |
| `.where(not(traversal))` | ✅ Implemented | Recursive `parseWhere()` support with predicate trees | N/A | **Done** | Covered by `translatesWhereNotPredicate` |
| `.match(as, …)` | Multiple correlated subqueries joined on shared alias | Requires parsing multiple anonymous `__.as(l)…` sub-traversals; `__` is not currently handled by the parser | Each pattern becomes a correlated subquery or JOIN; shared aliases become join keys | **Very High** | The `__` anonymous traversal syntax requires grammar and parser changes; arbitrary pattern combinations are not always reducible to joins |

---

### Group H — Branch and conditional gaps

| Step | SQL equivalent | Layer 2 changes | Layer 3 changes | Complexity | Key risk |
|------|---------------|-----------------|-----------------|------------|----------|
| `.coalesce(t1, t2)` | `COALESCE(expr1, expr2)` | Parse inner traversal arguments as sub-expressions (e.g., `values(p)` → column reference); add `coalesceExpressions` list to `ParsedTraversal` | In `buildVertexProjectionSql` or `buildHopTraversalSql` emit `COALESCE(col1, col2) AS alias` | **High** | Inner traversals must be simple `values(p)` or `constant(v)` steps — arbitrary inner traversals are not translatable |
| `.optional(traversal)` | `LEFT JOIN` instead of `INNER JOIN` for the traversal | Parse inner traversal as a single hop; add `optionalHop` flag to `HopStep` | Change `JOIN` to `LEFT JOIN` in the hop join loop when `optionalHop=true`; handle NULLs in SELECT | **Medium** | Only works when the optional traversal is a single hop — nested optionals or multi-hop optionals are not tractable |
| `.union(t1, t2, …)` | `SELECT … UNION ALL SELECT …` | Parse inner traversal arguments as sub-traversals; add `unionBranches` list to `ParsedTraversal` | Translate each branch separately and join with `UNION ALL`; wrap in outer SELECT if aggregation is needed | **High** | Each branch must be independently translatable; branches that share state with the outer traversal (aliased steps) require correlated rewriting |
| `.choose(pred, t_true, t_false)` general | `CASE WHEN … THEN … ELSE … END` | Extend `parseChooseProjection` beyond the current `values().is()` / `constant()` limitation; accept branch sub-traversals | In `buildVertexProjectionSql` emit CASE WHEN expressions; simple property branches map to column references | **High** | Branches that are multi-step traversals (not just `constant()` or `values()`) require recursive translation |

---

### Group I — Mutation steps

| Step | SQL equivalent | Layer 2 changes | Layer 3 changes | Complexity | Key risk |
|------|---------------|-----------------|-----------------|------------|----------|
| `addV(label)` | `INSERT INTO vertex_table …` | Requires detecting `addV` at root level (not in `parseSteps` but at `parse()` root); add a new `MutationResult` response type | New `buildInsertVertexSql()` method; must populate `idColumn` and all provided `property()` arguments | **High** | `/query/explain` returns SELECT SQL only; mutations would need a new endpoint or response mode; transaction safety is out of scope for the SQL translate path |
| `addE(label)` | `INSERT INTO edge_table …` | Same root-level detection as `addV` | New `buildInsertEdgeSql()`; requires `.from()` and `.to()` steps that have no current equivalents | **High** | Depends on `.from()`/`.to()` steps which are not in the current parse model at all |
| `drop()` | `DELETE FROM table WHERE id = ?` | Add `dropRequested` boolean; currently only makes sense after a filter traversal | New `buildDeleteSql()` in both vertex and edge paths | **High** | SQL `DELETE` requires a well-defined filter; `g.V().drop()` with no filter would be `DELETE FROM table` — extremely dangerous; must require explicit id or property filter |
| `.property(k, v)` | `UPDATE table SET col = ? WHERE id = ?` | Add `propertyUpdates` list to `ParsedTraversal`; set in `"property"` case | New `buildUpdateSql()` method | **High** | Same danger as `drop()` — an update without a filter is a full table update; must require filter before allowing property mutation |

---

### Group J — Analytics and other steps

| Step | SQL equivalent | Layer 2 changes | Layer 3 changes | Complexity | Key risk |
|------|---------------|-----------------|-----------------|------------|----------|
| `.pageRank()` | Not mappable — iterative algorithm; no standard SQL equivalent | N/A | Would require recursive CTEs with convergence logic — not portable | **Incompatible** | Graph algorithms requiring iterative convergence cannot be expressed in a single portable SQL query |
| `.peerPressure()` | Not mappable — iterative community detection | N/A | Same as `pageRank` | **Incompatible** | Same as `pageRank` |
| `.shortestPath()` | `WITH RECURSIVE cte AS (…)` | Would need recursive CTE support; add `shortestPathRequested` + source/target parameters | New `buildShortestPathSql()` emitting a recursive CTE; only works on databases that support `WITH RECURSIVE` (H2, PostgreSQL, DuckDB — not all Trino versions) | **Very High** | Recursive CTEs are not universally supported; depth-first vs. breadth-first semantics differ between SQL dialects |
| `.inject(v…)` | `SELECT ? UNION ALL SELECT ?…` (literal row injection) | Add `"inject"` case; store injected values; valid only at root position | Emit a `VALUES (…)` or `SELECT … UNION ALL` literal rows query | **Medium** | `inject()` is only meaningful at the root or as a seed for traversal; combining injected IDs with subsequent hops requires a subquery seed |
| `.constant(v)` standalone | `SELECT ? AS value` or used inside CASE WHEN | Standalone `constant()` outside `project.by` has no traversal context — would emit a single-literal SELECT | Emit `SELECT ? AS value` with the constant value as a parameter | **Low** | Useful mainly in `project.by(constant(…))` where it already works; standalone form has limited practical use |
| `.math(expr)` | `SELECT (col1 + col2) AS result` etc. | Parse the expression string; map property references to column names; add `mathExpression` field | Emit the substituted arithmetic expression in the SELECT clause | **High** | Expression parsing requires a mini-expression parser to substitute `_` (current value) and property names with mapped SQL columns; precedence and type safety are not validated |
| `.barrier()` | No-op | Add `"barrier"` case that does nothing | None | **Low** | SQL execution is already eager/set-oriented; `barrier()` is a TinkerPop pipeline hint with no SQL analogue |

---

## 19) Commercial Zero-ETL Graph DB Gremlin Parity Comparison

A commercial zero-ETL graph database solution is a read-only graph analytics engine that translates Gremlin traversals to queries against external data sources (Iceberg, Delta Lake, JDBC, etc.) — the same core use case as this engine's SQL translation mode. This section compares the two implementations step-by-step.

**Note:** The compared solution explicitly states it does not support Gremlin data manipulation (`addV`, `addE`, `drop`, `property`). Graph algorithms (PageRank, LPA, WCC, Louvain) are exposed via a separate `graph.program(…)` API and Cypher `CALL algo.paral.*` procedures — not as Gremlin steps.

### Legend

| Symbol | Meaning |
|--------|---------|
| ✅ Both | Both engines support this step |
| 🔷 commercial zero-ETL graph DB only | The commercial zero-ETL graph DB supports it; this engine does not in SQL mode |
| 🔵 This engine only | This engine supports it in SQL mode; not documented by the commercial zero-ETL graph DB |
| ❌ Neither | Neither engine supports this step in their SQL/translate mode |

---

### Traversal / hop steps

| Step | Status | Notes |
|------|--------|-------|
| `g.V()` / `g.E()` | ✅ Both | Core root steps |
| `g.V(id)` | ✅ Both | The commercial zero-ETL graph DB uses `label[id]` format IDs |
| `.out(l)` / `.in(l)` / `.both(l)` | ✅ Both | |
| `.outE(l)` / `.inE(l)` / `.bothE(l)` | ✅ Both | `bothE` supported as full hop step and in `where()` |
| `.outV()` / `.inV()` | ✅ Both | |
| `.bothV()` | ✅ Both | Supported after `g.E()` (union of out/in endpoints) |
| `.otherV()` | ✅ Both | Supported after `outE()/inE()` |
| `.repeat(…).times(n)` | ✅ Both | This engine expands to fixed-depth JOINs |
| `.until(…)` / `.emit()` | ❌ Neither | Variable-depth traversal is not expressible in plain SQL |

---

### Filter steps

| Step | Status | Notes |
|------|--------|-------|
| `.hasLabel(l)` | ✅ Both | |
| `.has(k, v)` / `.has(k, pred(v))` | ✅ Both | Both support `gt`, `gte`, `lt`, `lte`, `neq`, `eq` predicates |
| `.hasId(id)` | ✅ Both | Supported natively in SQL mode |
| `.hasNot(k)` | ✅ Both | Supported via `IS NULL` predicates |
| `.where(traversal)` | ✅ Both | commercial zero-ETL graph DB: `where(out('created'))`; this engine: edge-exist and neighbor-has correlated subquery forms |
| `.where(and(t1, t2))` / `.where(or(t1, t2))` | ✅ Both | Supported for recursive composition of traversal-style `where` predicates |
| `.where(not(traversal))` | ✅ Both | Supported as `NOT (...)` around traversal-style `where` predicates |
| `.is(predicate)` | ⚠️ Partial parity | Supported for `values(...).is(...)` and `where(select(...).is(...))`; general standalone forms remain unsupported |

---

### Projection / map steps

| Step | Status | Notes |
|------|--------|-------|
| `.values(p)` | ✅ Both | |
| `.valueMap()` | ✅ Both | Supported in SQL mode with mapping-backed property projection |
| `.project(a…).by(p)` | ✅ Both | commercial zero-ETL graph DB shows `project('person','knowsCount','createdCount').by(identity()).by(out('knows').count()).by(out('created').count())` |
| `.by(identity())` in `project` | ✅ Both | Supported for vertex and edge projections |
| `.by(out(l).count())` in `project` | ✅ Both | commercial zero-ETL graph DB uses this pattern; this engine supports it as `OUT_VERTEX_COUNT` projection kind |
| `.elementMap()` | ❌ Neither (documented) | Not mentioned in commercial zero-ETL graph DB docs; not in this engine |
| `.path()` | ✅ Both | commercial zero-ETL graph DB shows full traversal path output |
| `.path().by(p)` | 🔵 This engine only | This engine supports per-hop property extraction; commercial zero-ETL graph DB shows path without `by()` modulator |

---

### Aggregation steps

| Step | Status | Notes |
|------|--------|-------|
| `.count()` | ✅ Both | |
| `.dedup()` | ✅ Both | |
| `.groupCount().by(p)` | ✅ Both | |
| `.sum()` / `.mean()` | 🔵 This engine only | Not documented by the commercial zero-ETL graph DB as supported Gremlin steps |
| `.max()` / `.min()` | ❌ Neither (in SQL mode) | Neither engine documents these as SQL-translatable steps |
| `.fold()` / `.unfold()` | ❌ Neither (general) | This engine supports `fold()` only inside `by(out(l).values(p).fold())`; commercial zero-ETL graph DB does not document them |

---

### Ordering and paging steps

| Step | Status | Notes |
|------|--------|-------|
| `.order().by(p)` / `.order().by(p, desc)` | ✅ Both | |
| `.limit(n)` | ✅ Both | |
| `.range(lo, hi)` / `.skip(n)` / `.tail(n)` | ❌ Neither | Not documented by the commercial zero-ETL graph DB; not in this engine's SQL mode |

---

### Alias / select / where steps

| Step | Status | Notes |
|------|--------|-------|
| `.as(l)` / `.select(l…)` | ✅ Both | commercial zero-ETL graph DB shows `as('creator').out('created').select('creator').dedup()` |
| `.select(l).by(p)` | ✅ Both | |
| `.where(a, neq(b))` / `.where(eq(a))` | 🔵 This engine only | commercial zero-ETL graph DB does not document these specific `where()` forms |
| `.where(outE/inE/bothE(l).has(…))` | 🔵 This engine only | Edge-existence correlated subquery; not documented by the commercial zero-ETL graph DB |
| `.where(select(a).is(gt/gte(n)))` | 🔵 This engine only | Projected-alias filter; not documented by the commercial zero-ETL graph DB |

---

### Utilities

| Step | Status | Notes |
|------|--------|-------|
| `.profile()` | 🔷 commercial zero-ETL graph DB only | The commercial zero-ETL graph DB surfaces a query execution profile; this engine has no equivalent (SQL trace logging is the closest analogue) |
| `.identity()` | ✅ Both | Supported as no-op step and as `project(...).by(identity())` |
| `.simplePath()` | 🔵 This engine only | Not documented by the commercial zero-ETL graph DB |
| `.cyclicPath()` | ❌ Neither | |

---

### Mutation steps

| Step | Status | Notes |
|------|--------|-------|
| `addV` / `addE` / `drop` / `property` | ❌ Neither | Both engines explicitly do not support Gremlin-based data manipulation. The commercial zero-ETL graph DB states this in its introduction; this engine routes mutation via native TinkerGraph execution only |

---

### Graph algorithms

| Feature | commercial zero-ETL graph DB | This engine |
|---------|-------------------|-------------|
| PageRank | Via `graph.program(PageRankProgram…)` — separate API, not a Gremlin step | Not supported in SQL mode; available via TinkerGraph `pageRank()` step in native execution |
| Label Propagation | Via `graph.program(…)` API | Not supported |
| Weakly Connected Components | Via `graph.program(…)` API | Not supported |
| Louvain | Via `graph.program(…)` API | Not supported |
| Shortest Path | Not documented as a Gremlin step | Not supported in SQL mode |

Both engines treat graph algorithms as out-of-band from the Gremlin step pipeline. The commercial zero-ETL graph DB exposes them via a dedicated Java/Cypher API; this engine exposes them via native TinkerGraph execution.

---

### Summary

| Category | This engine ahead | commercial zero-ETL graph DB ahead | Parity |
|----------|------------------|------------------------|--------|
| Traversal hops | — | — | `out/in/both/outE/inE/bothE/outV/inV/bothV/otherV`, `repeat` |
| Filters | `where(neq/eq/projectGt)` forms | standalone `and()/or()/not()` filter steps, standalone `is()` | `has()`, `hasLabel()`, `hasId()`, `hasNot()`, `where(traversal)` |
| Projections | `by(outV/inV/edgeDegree/neighborFold/choose)`, `path().by(p)`, `sum/mean` | — | `project.by(p)`, `by(identity())`, `valueMap()`, `by(out.count)`, `as/select`, `dedup`, `count`, `order`, `limit` |
| Utilities | — | `profile()` | `path()`, `identity()` |

