#!/usr/bin/env python3
"""LSQB-inspired benchmark runner for GraphQueryEngine.

This script executes a weighted query workload against GraphQueryEngine endpoints
(e.g., /gremlin/query), then reports latency and throughput
metrics similar to graph benchmark scorecards.

It is intentionally lightweight and dependency-free (Python stdlib only).
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import statistics
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class QueryCase:
    name: str
    gremlin: str
    endpoint: str = "/gremlin/query"
    weight: int = 1


@dataclass
class RequestResult:
    query_name: str
    endpoint: str
    ok: bool
    status: int
    latency_ms: float
    error: str = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run LSQB-style benchmark workload against GraphQueryEngine")
    parser.add_argument("--base-url", default="http://localhost:7000", help="Base URL of GraphQueryEngine")
    parser.add_argument(
        "--workload",
        default="benchmarks/lsqb_aml_workload.json",
        help="Path to workload JSON file",
    )
    parser.add_argument("--clients", type=int, default=1, help="Number of parallel clients")
    parser.add_argument("--warmup", type=int, default=10, help="Warmup requests before measurement")
    parser.add_argument("--iterations", type=int, default=100, help="Measured requests to execute")
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=0,
        help="If > 0, run for this many seconds instead of fixed iterations",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=30,
        help="HTTP timeout per request",
    )
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        help="Extra HTTP header KEY=VALUE (repeatable)",
    )
    parser.add_argument(
        "--request-sql-trace",
        action="store_true",
        help="Send X-SQL-Trace: true header",
    )
    parser.add_argument(
        "--mapping-id",
        default="",
        help="Optional mapping id header (X-Mapping-Id)",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional path to save summary JSON",
    )
    parser.add_argument(
        "--output-csv",
        default="",
        help="Optional path to save per-request CSV",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for weighted query selection",
    )
    return parser.parse_args()


def load_workload(path: str) -> List[QueryCase]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "queries" not in payload:
        raise ValueError("Workload file must be an object with a 'queries' array")
    queries = payload["queries"]
    if not isinstance(queries, list) or not queries:
        raise ValueError("Workload 'queries' must be a non-empty array")

    cases: List[QueryCase] = []
    for i, q in enumerate(queries):
        if not isinstance(q, dict):
            raise ValueError(f"queries[{i}] must be an object")
        name = str(q.get("name", f"Q{i+1}"))
        gremlin = q.get("gremlin")
        if not isinstance(gremlin, str) or not gremlin.strip():
            raise ValueError(f"queries[{i}].gremlin must be a non-empty string")
        endpoint = str(q.get("endpoint", "/gremlin/query"))
        weight = int(q.get("weight", 1))
        if weight < 1:
            raise ValueError(f"queries[{i}].weight must be >= 1")
        cases.append(QueryCase(name=name, gremlin=gremlin, endpoint=endpoint, weight=weight))
    return cases


def parse_extra_headers(header_pairs: List[str]) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for pair in header_pairs:
        if "=" not in pair:
            raise ValueError(f"Invalid --header '{pair}', expected KEY=VALUE")
        k, v = pair.split("=", 1)
        k = k.strip()
        if not k:
            raise ValueError(f"Invalid --header '{pair}', key is empty")
        headers[k] = v
    return headers


def send_query(base_url: str, query_case: QueryCase, timeout_seconds: int, headers: Dict[str, str]) -> RequestResult:
    body = json.dumps({"gremlin": query_case.gremlin}).encode("utf-8")
    url = base_url.rstrip("/") + query_case.endpoint
    req = urllib.request.Request(url=url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    for k, v in headers.items():
        req.add_header(k, v)

    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            latency_ms = (time.perf_counter() - start) * 1000.0
            ok = 200 <= resp.status < 300
            if ok:
                try:
                    obj = json.loads(raw)
                    if isinstance(obj, dict) and "error" in obj:
                        return RequestResult(query_case.name, query_case.endpoint, False, resp.status, latency_ms, str(obj["error"]))
                except json.JSONDecodeError:
                    pass
            return RequestResult(query_case.name, query_case.endpoint, ok, resp.status, latency_ms, "")
    except urllib.error.HTTPError as e:
        latency_ms = (time.perf_counter() - start) * 1000.0
        msg = ""
        try:
            msg = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            msg = str(e)
        return RequestResult(query_case.name, query_case.endpoint, False, int(e.code), latency_ms, msg)
    except Exception as e:  # noqa: BLE001
        latency_ms = (time.perf_counter() - start) * 1000.0
        return RequestResult(query_case.name, query_case.endpoint, False, 0, latency_ms, str(e))


def percentile(sorted_values: List[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if p <= 0:
        return sorted_values[0]
    if p >= 100:
        return sorted_values[-1]
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return sorted_values[f]
    return sorted_values[f] + (sorted_values[c] - sorted_values[f]) * (k - f)


def summarize(results: List[RequestResult], elapsed_seconds: float) -> Dict[str, Any]:
    latencies = sorted(r.latency_ms for r in results)
    ok_count = sum(1 for r in results if r.ok)
    err_count = len(results) - ok_count
    throughput = ok_count / elapsed_seconds if elapsed_seconds > 0 else 0.0

    by_query: Dict[str, Dict[str, Any]] = {}
    for r in results:
        bucket = by_query.setdefault(r.query_name, {"count": 0, "ok": 0, "latencies": []})
        bucket["count"] += 1
        if r.ok:
            bucket["ok"] += 1
        bucket["latencies"].append(r.latency_ms)

    per_query = []
    for name, bucket in sorted(by_query.items()):
        qs = sorted(bucket["latencies"])
        per_query.append(
            {
                "name": name,
                "count": bucket["count"],
                "ok": bucket["ok"],
                "errorRatePct": round((bucket["count"] - bucket["ok"]) * 100.0 / bucket["count"], 3),
                "p50Ms": round(percentile(qs, 50), 3),
                "p95Ms": round(percentile(qs, 95), 3),
                "p99Ms": round(percentile(qs, 99), 3),
                "avgMs": round(statistics.fmean(qs), 3),
            }
        )

    return {
        "requests": len(results),
        "ok": ok_count,
        "errors": err_count,
        "errorRatePct": round((err_count * 100.0 / len(results)) if results else 0.0, 3),
        "elapsedSeconds": round(elapsed_seconds, 3),
        "throughputReqPerSec": round(throughput, 3),
        "latencyMs": {
            "min": round(latencies[0], 3) if latencies else 0.0,
            "p50": round(percentile(latencies, 50), 3),
            "p90": round(percentile(latencies, 90), 3),
            "p95": round(percentile(latencies, 95), 3),
            "p99": round(percentile(latencies, 99), 3),
            "max": round(latencies[-1], 3) if latencies else 0.0,
            "avg": round(statistics.fmean(latencies), 3) if latencies else 0.0,
        },
        "perQuery": per_query,
    }


def print_summary(summary: Dict[str, Any]) -> None:
    print("\n=== LSQB-Style Benchmark Summary ===")
    print(f"Requests:         {summary['requests']}")
    print(f"Successful:       {summary['ok']}")
    print(f"Errors:           {summary['errors']} ({summary['errorRatePct']}%)")
    print(f"Elapsed (sec):    {summary['elapsedSeconds']}")
    print(f"Throughput rps:   {summary['throughputReqPerSec']}")

    lat = summary["latencyMs"]
    print("Latency (ms):")
    print(
        f"  min={lat['min']} p50={lat['p50']} p90={lat['p90']} "
        f"p95={lat['p95']} p99={lat['p99']} max={lat['max']} avg={lat['avg']}"
    )

    print("\nPer-query:")
    for row in summary["perQuery"]:
        print(
            f"  {row['name']}: count={row['count']} ok={row['ok']} "
            f"err%={row['errorRatePct']} p50={row['p50Ms']} p95={row['p95Ms']} p99={row['p99Ms']} avg={row['avgMs']}"
        )


def build_headers(args: argparse.Namespace) -> Dict[str, str]:
    headers = parse_extra_headers(args.header)
    if args.request_sql_trace:
        headers["X-SQL-Trace"] = "true"
    if args.mapping_id:
        headers["X-Mapping-Id"] = args.mapping_id
    return headers


def weighted_cases(cases: List[QueryCase]) -> List[QueryCase]:
    expanded: List[QueryCase] = []
    for c in cases:
        expanded.extend([c] * c.weight)
    return expanded


def run_benchmark(args: argparse.Namespace, cases: List[QueryCase], headers: Dict[str, str]) -> List[RequestResult]:
    random.seed(args.seed)
    pool = weighted_cases(cases)
    if not pool:
        raise ValueError("Workload has no executable queries")

    # Warmup (sequential)
    for _ in range(max(args.warmup, 0)):
        case = random.choice(pool)
        _ = send_query(args.base_url, case, args.timeout_seconds, headers)

    measured: List[RequestResult] = []
    start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=max(args.clients, 1)) as ex:
        futures = []
        if args.duration_seconds > 0:
            end_at = start + args.duration_seconds
            while time.perf_counter() < end_at:
                while len(futures) < max(args.clients, 1) * 2 and time.perf_counter() < end_at:
                    case = random.choice(pool)
                    futures.append(ex.submit(send_query, args.base_url, case, args.timeout_seconds, headers))
                if futures:
                    done = futures.pop(0)
                    measured.append(done.result())
            for fut in futures:
                measured.append(fut.result())
        else:
            total = max(args.iterations, 1)
            for _ in range(total):
                case = random.choice(pool)
                futures.append(ex.submit(send_query, args.base_url, case, args.timeout_seconds, headers))
            for fut in as_completed(futures):
                measured.append(fut.result())

    return measured


def write_csv(path: str, results: List[RequestResult]) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["query", "endpoint", "ok", "status", "latencyMs", "error"])
        for r in results:
            writer.writerow([r.query_name, r.endpoint, r.ok, r.status, f"{r.latency_ms:.3f}", r.error])


def main() -> int:
    args = parse_args()
    cases = load_workload(args.workload)
    headers = build_headers(args)

    print("Loaded workload:")
    for c in cases:
        print(f"  - {c.name}: endpoint={c.endpoint} weight={c.weight}")

    t0 = time.perf_counter()
    results = run_benchmark(args, cases, headers)
    elapsed = time.perf_counter() - t0

    summary = summarize(results, elapsed)
    summary["config"] = {
        "baseUrl": args.base_url,
        "workload": args.workload,
        "clients": args.clients,
        "warmup": args.warmup,
        "iterations": args.iterations,
        "durationSeconds": args.duration_seconds,
        "timeoutSeconds": args.timeout_seconds,
    }

    print_summary(summary)

    if args.output_csv:
        write_csv(args.output_csv, results)
        print(f"\nWrote request CSV: {args.output_csv}")

    if args.output_json:
        out = Path(args.output_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"Wrote summary JSON: {args.output_json}")

    # Non-zero exit if any errors were observed.
    return 1 if summary["errors"] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())

