# Local Iceberg Stack (Docker)

This stack starts:
- MinIO (S3-compatible object store)
- Iceberg REST catalog
- Trino with an `iceberg` catalog

## Start

```zsh
cd /Users/vjoshi/SourceCode/graph-query-engine
chmod +x scripts/iceberg_local_up.sh scripts/iceberg_seed_trino.sh scripts/iceberg_local_down.sh
./scripts/iceberg_local_up.sh
```

## Seed demo tables

```zsh
cd /Users/vjoshi/SourceCode/graph-query-engine
./scripts/iceberg_seed_trino.sh
```

## Quick query via Trino

```zsh
docker exec -i iceberg-trino trino --server http://localhost:8080 --execute "SELECT * FROM iceberg.aml.accounts"
```

## Stop

```zsh
cd /Users/vjoshi/SourceCode/graph-query-engine
./scripts/iceberg_local_down.sh
```

