# Demo Guide

## Run the demo

**Terminal 1** — start the service:
```bash
mvn exec:java
```

**Terminal 2** — run the automated demo:
```bash
./demo.sh
```

The script runs 16 scenarios: basic traversals, 10-hop chains, path collection, transaction semantics, SQL explain mode, and the compatibility test suite.

## Manual curl examples

```bash
# Health
curl http://localhost:7000/health

# Count all vertices
curl -X POST http://localhost:7000/gremlin/query \
  -H "Content-Type: application/json" \
  -d '{"gremlin":"g.V().count()"}'

# 10-hop traversal
curl -X POST http://localhost:7000/gremlin/query \
  -H "Content-Type: application/json" \
  -d '{"gremlin":"g.V(1).repeat(out()).times(10).values(\"name\")"}'

# Transactional execution
curl -X POST http://localhost:7000/gremlin/query/tx \
  -H "Content-Type: application/json" \
  -d '{"gremlin":"g.V(1).values(\"name\")"}'

# SQL explain
curl -X POST http://localhost:7000/admin/seed-10hop
curl -X POST http://localhost:7000/mapping/upload \
  -F "file=@mappings/ten-hop-mapping.json"
curl -X POST http://localhost:7000/query/explain \
  -H "Content-Type: application/json" \
  -d '{"gremlin":"g.V(1).hasLabel(\"Node\").repeat(out(\"LINK\")).times(3)"}'
```

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `Connection refused` | Start service: `mvn exec:java` |
| `Port 7000 in use` | `PORT=8080 mvn exec:java` |
| `Permission denied: ./demo.sh` | `chmod +x ./demo.sh` |
| `jq: command not found` | `brew install jq` (optional, demo works without it) |
