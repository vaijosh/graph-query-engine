# Benchmarking (LSQB-Inspired)

This project now includes an LSQB-inspired benchmark runner for workload-based testing.

- Script: `scripts/benchmark_lsqb.py`
- Sample workload: `benchmarks/lsqb_aml_workload.json`

## What this provides

- Weighted multi-query workload execution
- Warmup + measured run phases
- Fixed-iterations mode or duration mode
- Parallel clients (`--clients`)
- Throughput and latency percentiles (p50/p90/p95/p99)
- Per-query breakdown
- Optional JSON summary and CSV request-level export

## Quick start

1) Start backend and load AML data/mapping first.

2) Run a short benchmark:

```bash
cd /Users/vjoshi/SourceCode/GraphQueryEngine
python3 scripts/benchmark_lsqb.py \
  --base-url http://localhost:7000 \
  --workload benchmarks/lsqb_aml_workload.json \
  --warmup 20 \
  --iterations 200 \
  --clients 4 \
  --mapping-id aml \
  --output-json output/bench-summary.json \
  --output-csv output/bench-requests.csv
```

## Duration mode example

```bash
cd /Users/vjoshi/SourceCode/GraphQueryEngine
python3 scripts/benchmark_lsqb.py \
  --base-url http://localhost:7000 \
  --workload benchmarks/lsqb_aml_workload.json \
  --warmup 30 \
  --duration-seconds 120 \
  --clients 8 \
  --mapping-id aml \
  --request-sql-trace
```

## Workload format

`queries` is a list of objects:

- `name`: label for reporting
- `gremlin`: traversal string
- `endpoint`: endpoint path (default `/gremlin/query`)
- `tx_mode`: optional bool; if true and endpoint omitted, defaults to `/gremlin/query/tx`
- `weight`: weighted selection frequency (default `1`)

Example:

```json
{
  "queries": [
    {
      "name": "count_accounts",
      "gremlin": "g.V().count()",
      "endpoint": "/gremlin/query",
      "weight": 5
    }
  ]
}
```

## Notes on LSQB alignment

This is LSQB-inspired benchmarking support, not a full official LSQB implementation.
It focuses on practical, repeatable workload testing for this service:

- query mix and weights
- latency and throughput metrics
- reproducible runs with a fixed random seed

If you want strict LSQB scenario mapping next, we can add named query suites and fixed phase definitions.

