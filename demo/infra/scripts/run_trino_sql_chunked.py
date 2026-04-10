#!/usr/bin/env python3
"""Execute a SQL file against Trino one statement at a time.

Uses the Trino Python client (direct HTTP) instead of `docker exec` per
statement to avoid the ~0.6 s container-spawn overhead on every call.

Falls back to docker exec if the trino package is not installed.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path


def split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    i = 0
    n = len(sql_text)

    while i < n:
        ch = sql_text[i]

        if ch == "'":
            # Handle escaped single quote in SQL string literal: ''
            if in_single_quote and i + 1 < n and sql_text[i + 1] == "'":
                current.append("''")
                i += 2
                continue
            in_single_quote = not in_single_quote
            current.append(ch)
            i += 1
            continue

        if ch == ";" and not in_single_quote:
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)

    return statements


# ── Trino-client executor (fast path) ────────────────────────────────────────

def _run_via_client(statements: list[str], server: str) -> None:
    """Execute all statements using the trino Python client (direct HTTP)."""
    import trino  # type: ignore

    # Parse host/port from server URL
    url = server.replace("http://", "").replace("https://", "")
    host, _, port_str = url.partition(":")
    port = int(port_str) if port_str else 8080

    conn = trino.dbapi.connect(
        host=host,
        port=port,
        user="admin",
        http_scheme="http",
        request_timeout=300,
    )

    total = len(statements)
    t_start = time.time()

    for idx, stmt in enumerate(statements, start=1):
        first_line = stmt.splitlines()[0][:80]
        print(f"[{idx}/{total}] {first_line}...", end=" ", flush=True)
        t0 = time.time()
        try:
            cur = conn.cursor()
            cur.execute(stmt)
            # Consume results so the query actually completes
            try:
                cur.fetchall()
            except Exception:
                pass
            elapsed = time.time() - t0
            print(f"OK ({elapsed:.1f}s)", flush=True)
        except Exception as exc:
            elapsed = time.time() - t0
            print(f"FAILED ({elapsed:.1f}s)")
            print(f"  ERROR: {exc}", file=sys.stderr)
            raise RuntimeError(f"Trino statement failed: {exc}") from exc

    total_time = time.time() - t_start
    print(f"\nAll {total} statements executed in {total_time:.1f}s.")


# ── docker-exec executor (fallback) ──────────────────────────────────────────

def _run_via_docker(statements: list[str], server: str, container: str) -> None:
    """Execute all statements via docker exec (original behaviour)."""
    total = len(statements)
    t_start = time.time()

    for idx, stmt in enumerate(statements, start=1):
        cmd = ["docker", "exec", container, "trino", "--server", server, "--execute", stmt]
        first_line = stmt.splitlines()[0][:80]
        print(f"[{idx}/{total}] {first_line}...", end=" ", flush=True)
        t0 = time.time()
        proc = subprocess.run(cmd, text=True, capture_output=True)
        elapsed = time.time() - t0

        if proc.returncode != 0:
            print(f"FAILED ({elapsed:.1f}s)")
            if proc.stderr:
                print(proc.stderr.rstrip())
            raise RuntimeError("Trino statement execution failed")

        print(f"OK ({elapsed:.1f}s)")
        if proc.stdout and proc.stdout.strip():
            print(f"  → {proc.stdout.rstrip()[:100]}")
        sys.stdout.flush()

    total_time = time.time() - t_start
    print(f"\nAll {total} statements executed in {total_time:.1f}s.")


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql", required=True, help="Path to SQL file")
    parser.add_argument("--server", default="http://localhost:8080", help="Trino server URL")
    parser.add_argument("--container", default="iceberg-trino",
                        help="Docker container name (used only for docker-exec fallback)")
    parser.add_argument("--docker-exec", action="store_true",
                        help="Force docker-exec mode even if trino package is installed")
    args = parser.parse_args()

    sql_path = Path(args.sql)
    if not sql_path.exists():
        raise SystemExit(f"SQL file not found: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_text)
    if not statements:
        print(f"No SQL statements found in: {sql_path}")
        return

    print(f"Executing {len(statements)} SQL statements from {sql_path}\n")
    sys.stdout.flush()

    if args.docker_exec:
        print("Mode: docker-exec (forced)\n")
        _run_via_docker(statements, server=args.server, container=args.container)
        return

    try:
        import trino  # noqa: F401
        print("Mode: trino Python client (direct HTTP)\n")
        _run_via_client(statements, server=args.server)
    except ImportError:
        print("trino package not found — falling back to docker-exec mode\n"
              "  (install with: pip install trino)\n")
        _run_via_docker(statements, server=args.server, container=args.container)


if __name__ == "__main__":
    main()

