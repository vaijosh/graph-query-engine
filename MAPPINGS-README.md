# Mapping Guide

This guide explains graph-to-relational mapping files used by the engine.

## Why mapping exists

Gremlin traversals are graph-native (vertices + edges), but execution targets SQL tables.
A mapping file tells the engine how to translate graph concepts into table/column joins.

## Top-level structure

```json
{
  "defaultDatasource": "h2",
  "vertices": {},
  "edges": {}
}
```

| Field | Required | Description |
|---|---|---|
| `vertices` | yes | One entry per vertex label |
| `edges` | yes | One entry per edge label |
| `defaultDatasource` | no | Backend id used when a vertex/edge has no own `datasource` field |

## Datasource routing

Each vertex and edge can optionally declare which registered backend it lives in:

```json
"Account": {
  "table": "aml_accounts",
  "datasource": "h2",
  ...
}
```

The engine resolves the backend in this priority order:

1. **Vertex/edge-level `datasource`** — most specific
2. **Mapping-level `defaultDatasource`** — mapping-wide fallback
3. **Registry default** — first backend registered at startup (always available)

This means you can have tables from H2 and Iceberg **in the same mapping** and the engine
will automatically connect to the right database for each query.

## Vertex mapping

```json
{
  "vertices": {
    "Account": {
      "table": "aml_accounts",
      "idColumn": "id",
      "datasource": "h2",
      "properties": {
        "accountId": "account_id",
        "bankId": "bank_id",
        "riskScore": "risk_score"
      }
    }
  }
}
```

| Field | Required | Description |
|---|---|---|
| `table` | yes | Physical SQL table (supports `iceberg:` prefix for catalog-qualified names) |
| `idColumn` | yes | Primary key column used as graph vertex id |
| `datasource` | no | Backend id (e.g. `"h2"`, `"iceberg"`) — overrides `defaultDatasource` |
| `properties` | no | Graph property name → table column name |

## Edge mapping

```json
{
  "edges": {
    "TRANSFER": {
      "table": "aml_transfers",
      "idColumn": "id",
      "outColumn": "from_account_id",
      "inColumn": "to_account_id",
      "outVertexLabel": "Account",
      "inVertexLabel": "Account",
      "datasource": "h2",
      "properties": {
        "amount": "amount",
        "currency": "currency",
        "isLaundering": "is_laundering"
      }
    }
  }
}
```

| Field | Required | Description |
|---|---|---|
| `table` | yes | Edge/link table |
| `idColumn` | yes | Edge primary key |
| `outColumn` | yes | Source endpoint id column in edge table |
| `inColumn` | yes | Destination endpoint id column in edge table |
| `outVertexLabel` | yes | Source vertex label |
| `inVertexLabel` | yes | Destination vertex label |
| `datasource` | no | Backend id — overrides `defaultDatasource` |
| `properties` | no | Edge property name → column name |

## Hybrid mapping (H2 + Iceberg in one file)

`aml-hybrid-mapping.json` shows how to mix backends in a single mapping.
Start the engine with both backends registered:

```bash
BACKENDS='[
  {"id":"h2",      "url":"jdbc:h2:file:./data/graph;AUTO_SERVER=TRUE"},
  {"id":"iceberg", "url":"jdbc:trino://localhost:8080/iceberg/aml","user":"admin"}
]' mvn exec:java
```

Upload the hybrid mapping:

```bash
curl -X POST http://localhost:7000/mapping/upload \
  -F "file=@demo/aml/mappings/aml-hybrid-mapping.json" \
  -F "id=hybrid" -F "name=AML Hybrid"
```

Now you can query either backend from the same mapping — the engine selects the right
connection automatically based on the `datasource` field of each vertex/edge:

```bash
# Queries Account vertex → routed to H2
curl -X POST http://localhost:7000/gremlin/query \
  -H "Content-Type: application/json" \
  -d '{"gremlin":"g.V().hasLabel(\"Account\").count()"}'

# Queries Bank vertex → routed to Iceberg
curl -X POST http://localhost:7000/gremlin/query \
  -H "Content-Type: application/json" \
  -d '{"gremlin":"g.V().hasLabel(\"Bank\").count()"}'
```

> **Note:** Cross-backend joins (traversals that hop from an H2 vertex to an Iceberg edge
> in a single SQL statement) are not supported — each Gremlin step is translated to a
> separate SQL query targeting one table. Multi-hop traversals that stay within one backend
> work normally.

## outColumn/inColumn and foreign keys

Think of each edge row as: `source_vertex_id --edge--> destination_vertex_id`.

- `outColumn` stores `source_vertex_id`
- `inColumn` stores `destination_vertex_id`

In SQL terms, these are FK-like references from the edge table to vertex tables.

## Traversal direction and joins

- `out('LABEL')`: join current vertex id to edge `outColumn`, then edge `inColumn` to next vertex
- `in('LABEL')`: reverse direction
- `outE('LABEL')` / `inE('LABEL')`: return edge rows while preserving source/destination semantics

## Table reference: `iceberg:` prefix

Prefix a table with `iceberg:` to target a catalog-qualified Iceberg table:

| Raw table value | Resolved SQL reference |
|---|---|
| `aml_accounts` | `aml_accounts` |
| `iceberg:iceberg.aml.accounts` | `iceberg.aml.accounts` |
| `iceberg:s3://warehouse/aml/accounts` | `iceberg_scan('s3://warehouse/aml/accounts')` |

When `datasource` is set to `"iceberg"`, the engine connects to the Trino backend;
the `iceberg:` prefix controls the SQL table reference format separately.

## Common mapping mistakes

- Using a property name in queries that is missing from `properties`
- Pointing `outVertexLabel`/`inVertexLabel` to wrong labels
- Swapping `outColumn` and `inColumn`
- Using different id domains between vertex `idColumn` and edge endpoint columns
- Forgetting to include a label used in Gremlin (`hasLabel`, `out`, `in`, `outE`, `inE`)
- Setting `datasource` to a backend id not registered in `BACKENDS` (falls back to default)

## Quick validation checklist

1. Every vertex label has `table` + `idColumn`.
2. Every edge label has `table`, `outColumn`, `inColumn`, and both endpoint labels.
3. Every queried property exists in the relevant `properties` map.
4. Edge endpoint columns hold ids from the configured vertex `idColumn`.
5. When using `datasource`, ensure the named backend id is registered at startup.
6. Run `/query/explain` first to confirm join direction before full query execution.

## Files in `mappings/`

| File | Description |
|---|---|
| `aml-mapping.json` | Default AML mapping — all tables in H2 |
| `iceberg-local-mapping.json` | All tables in local Trino/Iceberg |
| `aml-hybrid-mapping.json` | **Hybrid** — Account/TRANSFER in H2, Bank/Country/Alert in Iceberg |
| `ten-hop-mapping.json` | Stress/depth traversal example |

This guide explains graph-to-relational mapping files used by the engine.

## Why mapping exists

Gremlin traversals are graph-native (vertices + edges), but execution targets SQL tables.
A mapping file tells the engine how to translate graph concepts into table/column joins.

## Top-level structure

```json
{
  "vertices": {},
  "edges": {}
}
```

- `vertices`: one entry per vertex label (`Account`, `Bank`, etc.)
- `edges`: one entry per edge label (`TRANSFER`, `BELONGS_TO`, etc.)

## Vertex mapping

```json
{
  "vertices": {
    "Account": {
      "table": "iceberg:aml.accounts",
      "idColumn": "id",
      "properties": {
        "accountId": "account_id",
        "bankId": "bank_id",
        "riskScore": "risk_score"
      }
    }
  }
}
```

- `table`: physical SQL table (supports `iceberg:` prefix)
- `idColumn`: primary key column used as graph vertex id
- `properties`: graph property name -> table column name

## Edge mapping

```json
{
  "edges": {
    "TRANSFER": {
      "table": "iceberg:aml.transfers",
      "idColumn": "id",
      "outColumn": "out_id",
      "inColumn": "in_id",
      "outVertexLabel": "Account",
      "inVertexLabel": "Account",
      "properties": {
        "amount": "amount",
        "currency": "currency",
        "isLaundering": "is_laundering"
      }
    }
  }
}
```

- `table`: edge/link table
- `idColumn`: edge primary key
- `outColumn`: source endpoint id column in edge table
- `inColumn`: destination endpoint id column in edge table
- `outVertexLabel`: source vertex label
- `inVertexLabel`: destination vertex label
- `properties`: edge property name -> column name

## outColumn/inColumn and foreign keys

Think of each edge row as: `source_vertex_id --edge--> destination_vertex_id`.

- `outColumn` stores `source_vertex_id`
- `inColumn` stores `destination_vertex_id`

In SQL terms, these are FK-like references from the edge table to vertex tables.

For `BELONGS_TO` in `iceberg-local-mapping.json`:

- `account_bank.out_id` -> `accounts.id` (`Account`)
- `account_bank.in_id` -> `banks.id` (`Bank`)

For `TRANSFER`:

- `transfers.out_id` -> `accounts.id` (sender)
- `transfers.in_id` -> `accounts.id` (receiver)

## Traversal direction and joins

- `out('LABEL')`: join current vertex id to edge `outColumn`, then edge `inColumn` to next vertex
- `in('LABEL')`: reverse direction
- `outE('LABEL')` / `inE('LABEL')`: return edge rows while preserving source/destination semantics

If `outColumn` and `inColumn` are swapped in mapping, traversal direction is inverted.

## Logical schema from `iceberg-local-mapping.json`

Vertex tables:

- `aml.accounts(id, account_id, bank_id, risk_score)`
- `aml.banks(id, bank_id, bank_name, country_code)`
- `aml.countries(id, country_code, country_name, risk_level, region, fatf_blacklist)`
- `aml.alerts(id, alert_id, alert_type, severity, status, raised_at)`

Edge/link tables:

- `aml.transfers(id, out_id, in_id, amount, currency, is_laundering)`
- `aml.account_bank(id, out_id, in_id, is_primary)`
- `aml.bank_country(id, out_id, in_id, is_headquarters)`
- `aml.account_country(id, out_id, in_id, channel_type, routed_at)`
- `aml.account_alert(id, out_id, in_id, flagged_at, reason)`

## Common mapping mistakes

- Using a property name in queries that is missing from `properties`
- Pointing `outVertexLabel`/`inVertexLabel` to wrong labels
- Swapping `outColumn` and `inColumn`
- Using different id domains between vertex `idColumn` and edge endpoint columns
- Forgetting to include a label used in Gremlin (`hasLabel`, `out`, `in`, `outE`, `inE`)

## Quick validation checklist

1. Every vertex label has `table` + `idColumn`.
2. Every edge label has `table`, `outColumn`, `inColumn`, and both endpoint labels.
3. Every queried property exists in the relevant `properties` map.
4. Edge endpoint columns hold ids from the configured vertex `idColumn`.
5. Run `/query/explain` first to confirm join direction before full query execution.

## Files in `mappings/`

- `aml-mapping.json`: default AML mapping for local/H2 style tables
- `iceberg-mapping.json`: Iceberg-style mapping example
- `iceberg-local-mapping.json`: local Trino + Iceberg mapping used in notebook flows
- `ten-hop-mapping.json`: stress/depth traversal example mapping
