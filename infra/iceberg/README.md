# Local Iceberg Stack (Docker)

This stack starts:
- MinIO (S3-compatible object store)
- Iceberg REST catalog
- Trino with `iceberg` and `hive` catalogs

## Start

```zsh
cd /Users/vjoshi/SourceCode/graph-query-engine
chmod +x scripts/iceberg_local_up.sh scripts/iceberg_seed_trino_files.sh scripts/iceberg_local_down.sh
./scripts/iceberg_local_up.sh
```

## Seed demo tables (True file-based load - Recommended)

```zsh
cd /Users/vjoshi/SourceCode/graph-query-engine
./scripts/iceberg_seed_trino_files.sh --csv demo/data/aml-demo.csv --rows 100000 --format csv
```

This stages generated files to MinIO object storage and loads Iceberg tables from Hive external file tables.

You can also use JSON files:

```zsh
cd /Users/vjoshi/SourceCode/graph-query-engine
./scripts/iceberg_seed_trino_files.sh --csv demo/data/aml-demo.csv --rows 100000 --format json
```

## Legacy: Seed demo tables (INSERT VALUES Method)

For comparison or if you prefer the traditional approach:

```zsh
cd /Users/vjoshi/SourceCode/graph-query-engine
./scripts/iceberg_seed_trino.sh --csv demo/data/aml-demo.csv --rows 100000
```

## Quick query via Trino

```zsh
docker exec -i iceberg-trino trino --server http://localhost:8080 --execute "SELECT * FROM iceberg.aml.accounts"
```

## Quick query via Trino

```zsh
cd /Users/vjoshi/SourceCode/graph-query-engine
./scripts/iceberg_local_down.sh
```

