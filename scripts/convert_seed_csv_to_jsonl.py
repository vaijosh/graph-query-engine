#!/usr/bin/env python3
"""Convert per-table seed CSV files to newline-delimited JSON (JSONL)."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

TABLES = [
    "countries",
    "banks",
    "accounts",
    "transfers",
    "bank_country",
    "account_bank",
    "alerts",
    "account_country",
    "account_alert",
]


def convert_one(src: Path, dst: Path) -> int:
    rows = 0
    with src.open("r", encoding="utf-8", newline="") as f_in, dst.open("w", encoding="utf-8") as f_out:
        reader = csv.DictReader(f_in)
        for rec in reader:
            f_out.write(json.dumps(rec, ensure_ascii=True) + "\n")
            rows += 1
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-dir", required=True, help="Directory containing per-table CSV files")
    parser.add_argument("--out-dir", required=True, help="Output directory for per-table JSONL files")
    args = parser.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for table in TABLES:
        src = in_dir / f"{table}.csv"
        if not src.exists():
            raise SystemExit(f"Missing source CSV: {src}")
        dst = out_dir / f"{table}.json"
        rows = convert_one(src, dst)
        print(f"  - {table}: {rows} rows -> {dst.name}")


if __name__ == "__main__":
    main()

