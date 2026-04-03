# Graph Query Engine - Design Document

## 1) Purpose and Scope

The Graph Query Engine provides two complementary capabilities:

1. **Native Gremlin execution** via a graph provider (`/gremlin/query`, `/gremlin/query/tx`)
2. **Gremlin-to-SQL translation** for explain/reference (`/query/explain`)

The service is designed for a mapping-driven model where vertex/edge labels are mapped to relational tables and columns.

This document describes the current implementation in `src/main/java/com/graphqueryengine` and the current behavior covered by tests.

---

## 2) Top-Level Architecture

### Runtime Components

- **HTTP API layer**: `App` (Javalin routes, request parsing, error handling)
- **Mapping subsystem**: `MappingStore`, persisted mapping files, active mapping selection
- **Translation subsystem**:
  - parser abstraction: `GremlinTraversalParser`
  - parser implementations: legacy/manual and ANTLR
  - SQL compiler: `GremlinSqlTranslator`
  - mode wrappers: `StandardSqlGraphQueryTranslator`, `IcebergSqlGraphQueryTranslator`
- **Native execution subsystem**: `GremlinExecutionService` + `GraphProvider` SPI
- **Database bootstrap**: `DatabaseConfig`, `DatabaseManager`

### High-Level Data Flow

```text
Client Request
  -> Javalin route (App)
     -> Mapping resolution (active or X-Mapping-Id)
     -> Translator factory product (GraphQueryTranslator)
        -> Parser (ANTLR or legacy)
        -> GremlinSqlTranslator (dialect aware)
     -> JSON response with translated SQL + parameters
```

For native execution endpoints:

```text
Client Request
  -> Javalin route (App)
     -> GremlinExecutionService
        -> GraphProvider (TinkerGraph by default)
     -> JSON response with traversal results
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

### Admin/Seed Utilities

- `POST /admin/seed-demo`
- `POST /admin/seed-10hop`
- `POST /admin/seed-10hop-tx`
- `POST /admin/seed-gremlin-10hop-tx`
- `POST /admin/load-aml-csv?path=...&maxRows=...`

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
"BELONGS_TO": {
  "table": "iceberg:aml.account_bank",
  "idColumn": "id",
  "outColumn": "out_id",
  "inColumn":  "in_id",
  "outVertexLabel": "Account",
  "inVertexLabel":  "Bank",
  "properties": {}
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

Native Gremlin execution is routed through `GremlinExecutionService` and `GraphProvider` SPI (default provider: TinkerGraph).

This keeps SQL generation deterministic and testable while preserving full Gremlin semantics in native mode.

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

1. Add planner-stage debug output (structured plan JSON) before SQL rendering
2. Expand grammar support for additional Gremlin steps in SQL mode with explicit capability matrix
3. Introduce backend-specific SQL conformance tests (H2, Trino, DuckDB) from shared notebook workloads
4. Add docs for translator invariants (aliasing, parameterization, join ordering) to stabilize future refactors

