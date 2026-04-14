# Graph Query Engine — API Reference

## Request headers

| Header | Applies to | Description |
|--------|------------|-------------|
| `X-SQL-Trace: true\|false` | `/gremlin/query`, `/gremlin/query/tx`, `/query/explain` | Override SQL trace **logging** for this request (default: follows `SQL_TRACE` env var). Controls what is written to the **server log** — not what is returned in the response body. |
| `X-Mapping-Id: <id>` | `/query/explain`, `/mapping` | Use a specific stored mapping instead of the active one |



---

## Health

### `GET /health`
```
Returns: {"status":"ok","service":"graph-query-engine"}
```

---

## Gremlin execution

### `POST /gremlin/query`
Execute a Gremlin traversal natively via TinkerGraph.

**Body**
```json
{"gremlin": "<traversal>"}
```

**Response**
```json
{
  "gremlin": "g.V().count()",
  "results": [11],
  "resultCount": 1
}
```

**Examples**
```bash
curl -X POST http://localhost:7000/gremlin/query \
  -H "Content-Type: application/json" \
  -d '{"gremlin":"g.V().count()"}'

curl -X POST http://localhost:7000/gremlin/query \
  -H "Content-Type: application/json" \
  -d '{"gremlin":"g.V(1).repeat(out()).times(10).values(\"name\")"}'
```

---

### `POST /gremlin/query/tx`
Execute a Gremlin traversal within a transaction context.

**Body** — same as `/gremlin/query`

**Response**
```json
{
  "gremlin": "g.V(1).values(\"name\")",
  "results": ["Acct-1"],
  "resultCount": 1,
  "transactionMode": "NON_TRANSACTIONAL_GRAPH",
  "transactionStatus": "EXECUTED"
}
```

> `transactionMode` values: `NATIVE_GRAPH_TX` | `NON_TRANSACTIONAL_GRAPH`  
> `transactionStatus` values: `COMMITTED` | `EXECUTED`

---

### `GET /gremlin/provider`
```json
{"provider":"sql"}
```

---

## SQL explain

### `POST /query/explain`
Translate a Gremlin traversal to SQL without executing it. Requires an active (or header-selected) mapping.

**Headers (optional):** `X-Mapping-Id`, `X-SQL-Trace`

**Body**
```json
{"gremlin": "<traversal>"}
```

**Response**
```json
{
  "gremlin": "g.V(1).hasLabel('Node').repeat(out('LINK')).times(10)",
  "translatedSql": "SELECT v10.* FROM hop_nodes v0 JOIN hop_links e1 ON e1.out_id = v0.id ... WHERE v0.id = ? LIMIT ...",
  "parameters": ["1"],
  "mode": "SQL_EXPLAIN",
  "note": "This endpoint shows SQL translation for reference. Use /gremlin/query for actual execution. mappingId=..."
}
```

**Example**
```bash
curl -X POST http://localhost:7000/query/explain \
  -H "Content-Type: application/json" \
  -H "X-Mapping-Id: aml" \
  -d '{"gremlin":"g.V(1).hasLabel(\"Node\").repeat(out(\"LINK\")).times(10)"}'
```

---

### `POST /query` _(deprecated)_
Returns `307 Redirect → /gremlin/query` with a JSON body explaining the migration.

---

## Mapping management

### `POST /mapping/upload`
Upload a JSON mapping file.

**Query params**

| Param | Required | Description |
|-------|----------|-------------|
| `id` | No | Mapping ID (auto-generated UUID if omitted) |
| `name` | No | Human-readable name |
| `activate` | No | Set as active mapping (default: `true`) |

**Body** — `multipart/form-data`, field name `file`, JSON content:
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

**Response `201`**
```json
{
  "message": "Mapping loaded",
  "mappingId": "aml",
  "mappingName": "AML Mapping",
  "active": true,
  "vertexLabels": ["Account"],
  "edgeLabels": ["TRANSFER"],
  "createdAt": "2026-03-30T11:13:55.061265Z"
}
```

**Example**
```bash
curl -X POST "http://localhost:7000/mapping/upload?id=aml&name=AML+Mapping&activate=true" \
  -F "file=@mappings/aml-mapping.json;type=application/json"
```

---

### `GET /mapping`
Get the active mapping (or the mapping specified by `X-Mapping-Id`).

**Response**
```json
{
  "mappingId": "aml",
  "mappingName": "AML Mapping",
  "active": true,
  "createdAt": "2026-03-30T11:13:55.061265Z",
  "vertices": {},
  "edges": {}
}
```

---

### `GET /mappings`
List all stored mappings.

**Response**
```json
{
  "activeMappingId": "aml",
  "mappings": [
    {
      "id": "aml",
      "name": "AML Mapping",
      "active": true,
      "createdAt": "2026-03-30T11:13:55.061265Z",
      "vertexLabels": ["Account"],
      "edgeLabels": ["TRANSFER"]
    }
  ]
}
```

---

### `GET /mapping/status`
Summary of the mapping store.

**Response**
```json
{
  "totalMappings": 2,
  "activeMappingId": "aml",
  "activeMappingName": "AML Mapping",
  "activeVertexLabels": ["Account"],
  "activeEdgeLabels": ["TRANSFER"],
  "activeCreatedAt": "2026-03-30T11:13:55.061265Z",
  "mappingStoreDir": "/absolute/path/.mapping-store"
}
```

### Mapping selection behavior

- For endpoints that use mappings (for example `/query/explain`), `X-Mapping-Id` selects a specific mapping.
- If `X-Mapping-Id` is not provided, the active mapping is used.
- If neither is available, mapping-dependent operations return errors such as missing mapping / SQL translation unavailable.

---

### `POST /mapping/active?id=<id>`
Set the active mapping.

**Response**
```json
{"message": "Active mapping updated", "activeMappingId": "aml"}
```

---

### `DELETE /mapping?id=<id>`
Delete a stored mapping. If the deleted mapping was active, the next available mapping becomes active automatically.

**Response**
```json
{
  "message": "Mapping deleted",
  "deletedMappingId": "aml",
  "activeMappingId": null
}
```

---

## Environment variables

### Service
| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `7000` | HTTP listen port |
| `GRAPH_PROVIDER` | `sql` | Always `sql`. WCOJ acceleration is opt-out via `WCOJ_ENABLED=false`. |
| `SQL_TRACE` | `true` | Enable SQL trace logging globally |
| `MAPPING_STORE_DIR` | `.mapping-store` | Directory for persisted mapping files |

### Database (JDBC)
| Variable | Default | Description |
|----------|---------|-------------|
| `DB_URL` | `jdbc:h2:file:./data/graph;AUTO_SERVER=TRUE` | JDBC connection URL (H2 file-based; data persisted under `./data/graph.mv.db`) |
| `DB_USER` | `sa` | Database user |
| `DB_PASSWORD` | _(empty)_ | Database password |
| `DB_DRIVER` | `org.h2.Driver` | JDBC driver class |

---

## HTTP status codes

| Code | Meaning |
|------|---------|
| `200` | Success |
| `201` | Mapping uploaded |
| `307` | Redirect (`/query` → `/gremlin/query`) |
| `400` | Bad request (missing/invalid body, missing mapping) |
| `404` | Not found (mapping ID does not exist) |
| `500` | Server error (database failure, unexpected runtime error) |

---

## Error response shape

```json
{"error": "descriptive message here"}
```

---

## Supported Gremlin steps (SQL-translatable)

| Step | Notes |
|------|-------|
| `g.V()` / `g.E()` | Root traversal |
| `g.V(id)` | Root by ID |
| `.hasLabel(l)` | Vertex/edge label filter |
| `.has(k, v)` | Property equality filter |
| `.out(l)` / `.in(l)` / `.both(l)` | Vertex hop |
| `.outE(l)` / `.inE(l)` | Edge hop (in projections and `where`) |
| `.outV()` / `.inV()` | Vertex from edge |
| `.repeat(…).times(n)` | Fixed-depth loop |
| `.values(p)` | Property projection |
| `.limit(n)` | Result limit |
| `.count()` | Count result |
| `.dedup()` | Deduplicate |
| `.project(…).by(…)` | Multi-field projection |
| `.groupCount().by(p)` | Group-by count |
| `.order().by(select(…), Order.asc\|desc)` | Sort |
| `.as(l)` / `.select(l…).by(p)` | Named label + property select |
| `.where('a', neq('b'))` | Inequality between labels |
| `.where(outE/inE/bothE(l).has(…))` | Edge-existence filter |
| `.where(select(l).is(gt\|gte(n)))` | Projected value filter |
| `.path().by(p)` | Path extraction (SQL trace approximation) |
| `.simplePath()` | Cycle-free hint inside `repeat` |

Full TinkerPop semantics (`cyclicPath`, `match`, `sideEffect`, etc.) available via `/gremlin/query`.
