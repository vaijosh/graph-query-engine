#!/usr/bin/env python3
"""Generate SQL to load Iceberg tables from Hive external file tables (CSV or JSON)."""

from __future__ import annotations

import argparse
from pathlib import Path

TABLES = {
    "countries": {
        "raw_cols": ["id", "country_code", "country_name", "risk_level", "region", "fatf_blacklist"],
        "iceberg_cols": ["id", "country_code", "country_name", "risk_level", "region", "fatf_blacklist"],
        "select_expr": ["CAST(id AS BIGINT)", "country_code", "country_name", "risk_level", "region", "fatf_blacklist"],
    },
    "banks": {
        "raw_cols": ["id", "bank_id", "bank_name", "country_code"],
        "iceberg_cols": ["id", "bank_id", "bank_name", "country_code"],
        "select_expr": ["CAST(id AS BIGINT)", "bank_id", "bank_name", "country_code"],
    },
    "accounts": {
        "raw_cols": ["id", "account_id", "bank_id", "risk_score"],
        "iceberg_cols": ["id", "account_id", "bank_id", "risk_score"],
        "select_expr": ["CAST(id AS BIGINT)", "account_id", "bank_id", "CAST(risk_score AS DOUBLE)"],
    },
    "transfers": {
        "raw_cols": ["id", "out_id", "in_id", "amount", "currency", "is_laundering"],
        "iceberg_cols": ["id", "out_id", "in_id", "amount", "currency", "is_laundering"],
        "select_expr": [
            "CAST(id AS BIGINT)",
            "CAST(out_id AS BIGINT)",
            "CAST(in_id AS BIGINT)",
            "CAST(amount AS DOUBLE)",
            "currency",
            "is_laundering",
        ],
    },
    "bank_country": {
        "raw_cols": ["id", "out_id", "in_id", "is_headquarters"],
        "iceberg_cols": ["id", "out_id", "in_id", "is_headquarters"],
        "select_expr": ["CAST(id AS BIGINT)", "CAST(out_id AS BIGINT)", "CAST(in_id AS BIGINT)", "is_headquarters"],
    },
    "account_bank": {
        "raw_cols": ["id", "out_id", "in_id", "is_primary"],
        "iceberg_cols": ["id", "out_id", "in_id", "is_primary"],
        "select_expr": ["CAST(id AS BIGINT)", "CAST(out_id AS BIGINT)", "CAST(in_id AS BIGINT)", "is_primary"],
    },
    "alerts": {
        "raw_cols": ["id", "alert_id", "alert_type", "severity", "status", "raised_at"],
        "iceberg_cols": ["id", "alert_id", "alert_type", "severity", "status", "raised_at"],
        "select_expr": ["CAST(id AS BIGINT)", "alert_id", "alert_type", "severity", "status", "raised_at"],
    },
    "account_country": {
        "raw_cols": ["id", "out_id", "in_id", "channel_type", "routed_at"],
        "iceberg_cols": ["id", "out_id", "in_id", "channel_type", "routed_at"],
        "select_expr": ["CAST(id AS BIGINT)", "CAST(out_id AS BIGINT)", "CAST(in_id AS BIGINT)", "channel_type", "routed_at"],
    },
    "account_alert": {
        "raw_cols": ["id", "out_id", "in_id", "flagged_at", "reason"],
        "iceberg_cols": ["id", "out_id", "in_id", "flagged_at", "reason"],
        "select_expr": ["CAST(id AS BIGINT)", "CAST(out_id AS BIGINT)", "CAST(in_id AS BIGINT)", "flagged_at", "reason"],
    },
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, help="Output SQL file")
    parser.add_argument("--format", choices=["csv", "json"], default="csv")
    parser.add_argument("--raw-base", default="s3://aml/staging/seed_csv", help="Base S3 path containing per-table folders")
    args = parser.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    fmt = args.format.upper()
    raw_schema = "hive.aml_raw"

    lines: list[str] = []
    lines.append("CREATE SCHEMA IF NOT EXISTS iceberg.aml;")
    lines.append("CREATE SCHEMA IF NOT EXISTS hive.aml_raw WITH (location='s3://aml/staging/');")
    lines.append("")

    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.accounts (id BIGINT, account_id VARCHAR, bank_id VARCHAR, risk_score DOUBLE);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.banks (id BIGINT, bank_id VARCHAR, bank_name VARCHAR, country_code VARCHAR);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.countries (id BIGINT, country_code VARCHAR, country_name VARCHAR, risk_level VARCHAR, region VARCHAR, fatf_blacklist VARCHAR);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.transfers (id BIGINT, out_id BIGINT, in_id BIGINT, amount DOUBLE, currency VARCHAR, is_laundering VARCHAR);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.bank_country (id BIGINT, out_id BIGINT, in_id BIGINT, is_headquarters VARCHAR);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.account_bank (id BIGINT, out_id BIGINT, in_id BIGINT, is_primary VARCHAR);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.alerts (id BIGINT, alert_id VARCHAR, alert_type VARCHAR, severity VARCHAR, status VARCHAR, raised_at VARCHAR);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.account_country (id BIGINT, out_id BIGINT, in_id BIGINT, channel_type VARCHAR, routed_at VARCHAR);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.account_alert (id BIGINT, out_id BIGINT, in_id BIGINT, flagged_at VARCHAR, reason VARCHAR);")
    lines.append("")

    for table, spec in TABLES.items():
        raw_cols = ", ".join(f"{c} VARCHAR" for c in spec["raw_cols"])
        ext_loc = f"{args.raw_base.rstrip('/')}/{table}/"
        lines.append(f"DROP TABLE IF EXISTS {raw_schema}.{table}_raw;")
        if args.format == "csv":
            lines.append(
                f"CREATE TABLE {raw_schema}.{table}_raw ({raw_cols}) WITH (external_location='{ext_loc}', format='{fmt}', skip_header_line_count=1);"
            )
        else:
            lines.append(
                f"CREATE TABLE {raw_schema}.{table}_raw ({raw_cols}) WITH (external_location='{ext_loc}', format='{fmt}');"
            )
    lines.append("")

    for table in TABLES:
        lines.append(f"DELETE FROM iceberg.aml.{table};")
    lines.append("")

    for table, spec in TABLES.items():
        target_cols = ", ".join(spec["iceberg_cols"])
        select_expr = ", ".join(spec["select_expr"])
        lines.append(f"INSERT INTO iceberg.aml.{table} ({target_cols})")
        lines.append(f"SELECT {select_expr} FROM {raw_schema}.{table}_raw;")
        lines.append("")

    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote SQL: {out}")


if __name__ == "__main__":
    main()

