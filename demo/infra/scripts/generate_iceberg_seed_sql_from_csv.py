#!/usr/bin/env python3
"""Generate Iceberg seed SQL from aml-demo.csv style input."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


COUNTRIES = [
    (201, "US", "United States", "LOW", "Americas", "false"),
    (202, "GB", "United Kingdom", "LOW", "Europe", "false"),
    (203, "DE", "Germany", "LOW", "Europe", "false"),
    (204, "CH", "Switzerland", "MEDIUM", "Europe", "false"),
    (205, "HK", "Hong Kong", "MEDIUM", "Asia", "false"),
    (206, "SG", "Singapore", "LOW", "Asia", "false"),
    (207, "AE", "UAE", "MEDIUM", "Middle East", "false"),
    (208, "NG", "Nigeria", "HIGH", "Africa", "false"),
    (209, "KY", "Cayman Islands", "HIGH", "Americas", "true"),
    (210, "PA", "Panama", "HIGH", "Americas", "true"),
]


def java_hashcode(text: str) -> int:
    h = 0
    for ch in text:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    if h >= 0x80000000:
        h -= 0x100000000
    return h


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def emit_insert(lines: list[str], table: str, columns: list[str], rows: list[tuple], batch_size: int = 1000) -> None:
    if not rows:
        return
    cols = ", ".join(columns)
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        values_sql = []
        for row in chunk:
            parts = []
            for val in row:
                if isinstance(val, (int, float)):
                    parts.append(str(val))
                else:
                    parts.append(sql_quote(str(val)))
            values_sql.append("(" + ", ".join(parts) + ")")
        lines.append(f"INSERT INTO {table} ({cols}) VALUES")
        lines.append("  " + ",\n  ".join(values_sql) + ";")


def build_rows(csv_path: Path, max_rows: int):
    country_by_code = {code: c for c in COUNTRIES for code in [c[1]]}

    banks: dict[str, int] = {}
    accounts: dict[str, int] = {}

    bank_rows: list[tuple] = []
    account_rows: list[tuple] = []
    transfer_rows: list[tuple] = []
    account_bank_rows: list[tuple] = []
    bank_country_rows: list[tuple] = []
    alert_rows: list[tuple] = []
    account_country_rows: list[tuple] = []
    account_alert_rows: list[tuple] = []

    bank_country_seen: set[tuple[int, int]] = set()
    account_bank_seen: set[tuple[int, int]] = set()
    account_country_seen: set[tuple[int, int]] = set()

    next_bank_id = 1001
    next_account_id = 1
    next_transfer_id = 1
    next_edge_id = 1
    next_alert_pk = 1

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, rec in enumerate(reader):
            if idx >= max_rows:
                break

            from_bank = rec["from_bank"].strip()
            from_account = rec["from_account"].strip()
            to_bank = rec["to_bank"].strip()
            to_account = rec["to_account"].strip()
            amount = float(rec["amount_paid"].strip() or "0")
            currency = rec["payment_currency"].strip() or "USD"
            ts = rec["timestamp"].strip() or "1970-01-01T00:00:00Z"
            laundering = rec.get("is_laundering", "0").strip() or "0"
            tx_id = rec.get("transaction_id", "").strip() or str(next_transfer_id)
            payment_format = rec.get("payment_format", "UNKNOWN").strip() or "UNKNOWN"

            for bank_key in (from_bank, to_bank):
                if bank_key not in banks:
                    bank_id = next_bank_id
                    next_bank_id += 1
                    banks[bank_key] = bank_id
                    cc_idx = abs(java_hashcode(bank_key)) % len(COUNTRIES)
                    cc = COUNTRIES[cc_idx]
                    bank_rows.append((bank_id, bank_key, f"Bank-{bank_key}", cc[1]))
                    pair = (bank_id, cc[0])
                    if pair not in bank_country_seen:
                        bank_country_seen.add(pair)
                        bank_country_rows.append((next_edge_id, bank_id, cc[0], "true"))
                        next_edge_id += 1

            from_bank_id = banks[from_bank]
            to_bank_id = banks[to_bank]

            from_key = f"{from_bank}:{from_account}"
            to_key = f"{to_bank}:{to_account}"

            for acct_key, acct_id_val, bank_id_val in (
                (from_key, from_account, from_bank_id),
                (to_key, to_account, to_bank_id),
            ):
                if acct_key not in accounts:
                    account_id = next_account_id
                    next_account_id += 1
                    accounts[acct_key] = account_id
                    # Derive a deterministic risk score spread across all accounts
                    h = abs(java_hashcode(acct_key))
                    bucket = h % 100
                    if bucket < 5:         # ~5 % extremely high risk → blocked
                        risk_score = round(0.85 + (h % 15) / 100.0, 2)
                        is_blocked = "true"
                    elif bucket < 20:      # ~15 % high risk, not blocked
                        risk_score = round(0.71 + (h % 14) / 100.0, 2)
                        is_blocked = "false"
                    else:                  # ~80 % normal
                        risk_score = round(0.05 + (h % 65) / 100.0, 2)
                        is_blocked = "false"
                    # Derive opened_date from the transaction timestamp
                    opened_date = ts[:10].replace("/", "-") if ts else "2020-01-01"
                    acct_type = "BUSINESS" if h % 3 == 0 else "PERSONAL"
                    account_rows.append((account_id, acct_id_val, acct_key.split(":", 1)[0], risk_score,
                                         is_blocked, opened_date, acct_type))

                    pair = (account_id, bank_id_val)
                    if pair not in account_bank_seen:
                        account_bank_seen.add(pair)
                        account_bank_rows.append((next_edge_id, account_id, bank_id_val, "true"))
                        next_edge_id += 1

            from_account_pk = accounts[from_key]
            to_account_pk = accounts[to_key]

            transfer_rows.append((next_transfer_id, from_account_pk, to_account_pk,
                                   tx_id, amount, currency, payment_format, ts, laundering))
            next_transfer_id += 1

            to_bank_cc_idx = abs(java_hashcode(to_bank)) % len(COUNTRIES)
            to_country_id = COUNTRIES[to_bank_cc_idx][0]
            pair = (from_account_pk, to_country_id)
            if pair not in account_country_seen:
                account_country_seen.add(pair)
                account_country_rows.append((next_edge_id, from_account_pk, to_country_id, payment_format, ts))
                next_edge_id += 1

            if laundering == "1":
                severity = "HIGH" if amount > 50000 else "MEDIUM"
                alert_pk = next_alert_pk + 500000
                next_alert_pk += 1
                alert_rows.append((alert_pk, f"ALERT-{tx_id}", "SUSPICIOUS_TRANSFER", severity, "OPEN", ts))
                account_alert_rows.append((next_edge_id, from_account_pk, alert_pk, ts, "Suspicious outbound transfer"))
                next_edge_id += 1

    country_rows = [
        (c[0], c[1], c[2], c[3], c[4], c[5])
        for c in COUNTRIES
    ]

    return {
        "countries": country_rows,
        "banks": bank_rows,
        "accounts": account_rows,
        "transfers": transfer_rows,
        "bank_country": bank_country_rows,
        "account_bank": account_bank_rows,
        "alerts": alert_rows,
        "account_country": account_country_rows,
        "account_alert": account_alert_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to aml-demo.csv style file")
    parser.add_argument("--out", required=True, help="Output SQL file path")
    parser.add_argument("--rows", type=int, default=100000, help="Max CSV rows to ingest")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV file not found: {csv_path}")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows = build_rows(csv_path, args.rows)

    lines: list[str] = []
    lines.append("CREATE SCHEMA IF NOT EXISTS iceberg.aml;")
    lines.append("")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.accounts (id BIGINT, account_id VARCHAR, bank_id VARCHAR, risk_score DOUBLE, is_blocked VARCHAR, opened_date VARCHAR, account_type VARCHAR);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.banks (id BIGINT, bank_id VARCHAR, bank_name VARCHAR, country_code VARCHAR);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.countries (id BIGINT, country_code VARCHAR, country_name VARCHAR, risk_level VARCHAR, region VARCHAR, fatf_blacklist VARCHAR);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.transfers (id BIGINT, out_id BIGINT, in_id BIGINT, transaction_id VARCHAR, amount DOUBLE, currency VARCHAR, payment_format VARCHAR, event_time VARCHAR, is_laundering VARCHAR);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.bank_country (id BIGINT, out_id BIGINT, in_id BIGINT, is_headquarters VARCHAR);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.account_bank (id BIGINT, out_id BIGINT, in_id BIGINT, is_primary VARCHAR);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.alerts (id BIGINT, alert_id VARCHAR, alert_type VARCHAR, severity VARCHAR, status VARCHAR, raised_at VARCHAR);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.account_country (id BIGINT, out_id BIGINT, in_id BIGINT, channel_type VARCHAR, routed_at VARCHAR);")
    lines.append("CREATE TABLE IF NOT EXISTS iceberg.aml.account_alert (id BIGINT, out_id BIGINT, in_id BIGINT, flagged_at VARCHAR, reason VARCHAR);")
    lines.append("")
    lines.append("ALTER TABLE iceberg.aml.transfers ADD COLUMN IF NOT EXISTS is_laundering VARCHAR;")
    lines.append("ALTER TABLE iceberg.aml.transfers ADD COLUMN IF NOT EXISTS transaction_id VARCHAR;")
    lines.append("ALTER TABLE iceberg.aml.transfers ADD COLUMN IF NOT EXISTS payment_format VARCHAR;")
    lines.append("ALTER TABLE iceberg.aml.transfers ADD COLUMN IF NOT EXISTS event_time VARCHAR;")
    lines.append("ALTER TABLE iceberg.aml.banks ADD COLUMN IF NOT EXISTS country_code VARCHAR;")
    lines.append("ALTER TABLE iceberg.aml.accounts ADD COLUMN IF NOT EXISTS is_blocked VARCHAR;")
    lines.append("ALTER TABLE iceberg.aml.accounts ADD COLUMN IF NOT EXISTS opened_date VARCHAR;")
    lines.append("ALTER TABLE iceberg.aml.accounts ADD COLUMN IF NOT EXISTS account_type VARCHAR;")
    lines.append("")
    for table in [
        "accounts",
        "banks",
        "countries",
        "transfers",
        "bank_country",
        "account_bank",
        "alerts",
        "account_country",
        "account_alert",
    ]:
        lines.append(f"DELETE FROM iceberg.aml.{table};")
    lines.append("")

    emit_insert(lines, "iceberg.aml.countries", ["id", "country_code", "country_name", "risk_level", "region", "fatf_blacklist"], rows["countries"])
    emit_insert(lines, "iceberg.aml.banks", ["id", "bank_id", "bank_name", "country_code"], rows["banks"])
    emit_insert(lines, "iceberg.aml.accounts", ["id", "account_id", "bank_id", "risk_score", "is_blocked", "opened_date", "account_type"], rows["accounts"])
    emit_insert(lines, "iceberg.aml.transfers", ["id", "out_id", "in_id", "transaction_id", "amount", "currency", "payment_format", "event_time", "is_laundering"], rows["transfers"])
    emit_insert(lines, "iceberg.aml.bank_country", ["id", "out_id", "in_id", "is_headquarters"], rows["bank_country"])
    emit_insert(lines, "iceberg.aml.account_bank", ["id", "out_id", "in_id", "is_primary"], rows["account_bank"])
    emit_insert(lines, "iceberg.aml.alerts", ["id", "alert_id", "alert_type", "severity", "status", "raised_at"], rows["alerts"])
    emit_insert(lines, "iceberg.aml.account_country", ["id", "out_id", "in_id", "channel_type", "routed_at"], rows["account_country"])
    emit_insert(lines, "iceberg.aml.account_alert", ["id", "out_id", "in_id", "flagged_at", "reason"], rows["account_alert"])

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote SQL: {out_path}")
    print(f"Rows -> accounts={len(rows['accounts'])} transfers={len(rows['transfers'])} alerts={len(rows['alerts'])}")


if __name__ == "__main__":
    main()

