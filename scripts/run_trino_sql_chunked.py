#!/usr/bin/env python3
"""Execute a SQL file against Trino one statement at a time to avoid CLI OOM on large files."""

from __future__ import annotations

import argparse
import subprocess
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


def run_statement(statement: str, server: str, container: str) -> None:
    cmd = [
        "docker",
        "exec",
        container,
        "trino",
        "--server",
        server,
        "--execute",
        statement,
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True)
    if proc.stdout:
        print(proc.stdout.rstrip())
    if proc.returncode != 0:
        if proc.stderr:
            print(proc.stderr.rstrip())
        raise RuntimeError("Trino statement execution failed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql", required=True, help="Path to SQL file")
    parser.add_argument("--server", default="http://localhost:8080", help="Trino server URL")
    parser.add_argument("--container", default="iceberg-trino", help="Docker container name running Trino CLI")
    args = parser.parse_args()

    sql_path = Path(args.sql)
    if not sql_path.exists():
        raise SystemExit(f"SQL file not found: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_text)
    if not statements:
        print(f"No SQL statements found in: {sql_path}")
        return

    print(f"Executing {len(statements)} SQL statements from {sql_path}")
    for idx, statement in enumerate(statements, start=1):
        first_line = statement.splitlines()[0][:80]
        print(f"[{idx}/{len(statements)}] {first_line}")
        run_statement(statement, server=args.server, container=args.container)

    print("All statements executed successfully.")


if __name__ == "__main__":
    main()

