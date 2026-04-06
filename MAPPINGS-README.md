# Mapping Guide

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
