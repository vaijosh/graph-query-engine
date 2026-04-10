#!/usr/bin/env python3
"""
seed_iceberg_from_csv.py
------------------------
Bulk-loads AML (or any graph) CSV data into Iceberg tables via MinIO + Trino.

Instead of generating thousands of SQL INSERT statements (one per row), this
script:

  1. Parses the source CSV in Python (same logic as generate_iceberg_seed_sql_from_csv.py)
  2. Writes per-table CSV files to a MinIO bucket using the boto3 / minio SDK
     (falls back to mc CLI if SDK not installed)
  3. Creates a temporary Hive *external* table pointing at the CSV in MinIO
  4. Runs one  INSERT INTO iceberg_table SELECT * FROM hive_csv_table  per table
  5. Drops the temporary Hive external table

This approach is 5–50x faster than statement-per-row for large datasets.

Usage:
    # AML dataset (default schema=aml, catalog=iceberg)
    python3 demo/infra/scripts/seed_iceberg_from_csv.py --csv data/aml-demo.csv

    # With wipe (drop + recreate tables)
    python3 demo/infra/scripts/seed_iceberg_from_csv.py --csv data/aml-demo.csv --wipe

    # Limit rows (useful for quick tests)
    python3 demo/infra/scripts/seed_iceberg_from_csv.py --csv data/aml-demo.csv --rows 5000

    # Social-network dataset
    python3 demo/infra/scripts/seed_iceberg_from_csv.py --dataset social_network

Requirements (optional but recommended):
    pip install boto3 trino

If boto3/trino are not installed the script falls back to:
    - mc (MinIO CLI) for file uploads
    - docker exec for Trino SQL execution
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

# ── constants ────────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET     = "warehouse"

TRINO_HOST      = "localhost"
TRINO_PORT      = 8080
TRINO_USER      = "admin"
TRINO_CONTAINER = "iceberg-trino"

COUNTRIES = [
    (201, "US", "United States",  "LOW",    "Americas",    "false"),
    (202, "GB", "United Kingdom", "LOW",    "Europe",      "false"),
    (203, "DE", "Germany",        "LOW",    "Europe",      "false"),
    (204, "CH", "Switzerland",    "MEDIUM", "Europe",      "false"),
    (205, "HK", "Hong Kong",      "MEDIUM", "Asia",        "false"),
    (206, "SG", "Singapore",      "LOW",    "Asia",        "false"),
    (207, "AE", "UAE",            "MEDIUM", "Middle East", "false"),
    (208, "NG", "Nigeria",        "HIGH",   "Africa",      "false"),
    (209, "KY", "Cayman Islands", "HIGH",   "Americas",    "true"),
    (210, "PA", "Panama",         "HIGH",   "Americas",    "true"),
]


def _java_hashcode(text: str) -> int:
    h = 0
    for ch in text:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    if h >= 0x80000000:
        h -= 0x100000000
    return h


# ── CSV helpers ──────────────────────────────────────────────────────────────

def _write_csv_bytes(fieldnames: list[str], rows: list[tuple]) -> bytes:
    """Return a CSV (as bytes) for upload to MinIO."""
    buf = io.StringIO()
    w = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    w.writerow(fieldnames)
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


# ── MinIO upload ─────────────────────────────────────────────────────────────

def _upload_via_boto3(data: bytes, bucket: str, key: str) -> None:
    """Upload using boto3 (S3-compatible API)."""
    import boto3  # type: ignore
    import boto3.session  # type: ignore
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=boto3.session.Config(signature_version="s3v4"),
    )
    # Create bucket if it doesn't exist
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        try:
            s3.create_bucket(Bucket=bucket)
        except Exception:
            pass  # may already exist (race condition)
    s3.put_object(Bucket=bucket, Key=key, Body=data)


def _upload_via_minio_sdk(data: bytes, bucket: str, key: str) -> None:
    """Upload using the minio Python SDK (pip install minio)."""
    import io
    from minio import Minio  # type: ignore
    from minio.error import S3Error  # type: ignore

    # Parse host:port from MINIO_ENDPOINT
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    secure = MINIO_ENDPOINT.startswith("https://")

    client = Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
    )
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    client.put_object(bucket, key, io.BytesIO(data), length=len(data),
                      content_type="text/csv")


def _upload_via_mc(data: bytes, bucket: str, key: str) -> None:
    """Upload using the mc CLI (MinIO client) — last resort fallback."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tf:
        tf.write(data)
        tmp_path = tf.name
    try:
        subprocess.run(
            ["mc", "alias", "set", "local", MINIO_ENDPOINT,
             MINIO_ACCESS_KEY, MINIO_SECRET_KEY, "--api", "s3v4"],
            check=True, capture_output=True,
        )
        subprocess.run(
            ["mc", "cp", tmp_path, f"local/{bucket}/{key}"],
            check=True, capture_output=True,
        )
    finally:
        os.unlink(tmp_path)


def upload_csv(data: bytes, bucket: str, key: str) -> None:
    """
    Upload CSV bytes to MinIO.  Tries three methods in order:
      1. boto3   (pip install boto3)
      2. minio   (pip install minio)
      3. mc CLI  (install from https://min.io/docs/minio/linux/reference/minio-mc.html)

    Install the recommended dependencies with:
      pip install boto3 minio
    """
    # 1. Try boto3
    try:
        import boto3  # noqa: F401
        print(f"  Uploading s3://{bucket}/{key} via boto3 …", end=" ", flush=True)
        _upload_via_boto3(data, bucket, key)
        print("done")
        return
    except ImportError:
        pass
    except Exception as e:
        print(f"FAILED (boto3 error: {e})", flush=True)
        # fall through to minio SDK

    # 2. Try minio Python SDK
    try:
        from minio import Minio  # noqa: F401
        print(f"  Uploading s3://{bucket}/{key} via minio SDK …", end=" ", flush=True)
        _upload_via_minio_sdk(data, bucket, key)
        print("done")
        return
    except ImportError:
        pass
    except Exception as e:
        print(f"FAILED (minio SDK error: {e})", flush=True)
        # fall through to mc CLI

    # 3. Try mc CLI
    print(f"  Uploading s3://{bucket}/{key} via mc CLI …", end=" ", flush=True)
    try:
        _upload_via_mc(data, bucket, key)
        print("done")
    except Exception as e:
        raise RuntimeError(
            f"All upload methods failed for s3://{bucket}/{key}.\n"
            f"mc CLI error: {e}\n\n"
            "Install one of:\n"
            "  pip install boto3      (recommended)\n"
            "  pip install minio\n"
            "  brew install minio/stable/mc  (mc CLI)"
        ) from e


# ── Trino execution ──────────────────────────────────────────────────────────

def _trino_via_client(sql: str) -> list[Any]:
    """Execute SQL via the trino Python client and return rows."""
    import trino  # type: ignore
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        http_scheme="http",
        request_timeout=600,
    )
    cur = conn.cursor()
    cur.execute(sql)
    try:
        return cur.fetchall()
    except Exception:
        return []


def _trino_via_docker(sql: str) -> None:
    cmd = ["docker", "exec", TRINO_CONTAINER, "trino",
           "--server", f"http://localhost:{TRINO_PORT}",
           "--execute", sql]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if r.returncode != 0:
        raise RuntimeError(f"Trino failed: {(r.stderr or r.stdout)[:400]}")


def trino_exec(sql: str) -> list[Any]:
    """Execute SQL against Trino, using Python client if available."""
    try:
        import trino  # noqa: F401
        return _trino_via_client(sql)
    except ImportError:
        _trino_via_docker(sql)
        return []


def trino_count(sql: str) -> int:
    try:
        rows = trino_exec(sql)
        if rows:
            return int(rows[0][0])
    except Exception:
        pass
    return -1


# ── AML data builder (same logic as generate_iceberg_seed_sql_from_csv.py) ──

def build_aml_tables(csv_path: Path, max_rows: int) -> dict[str, tuple[list, list]]:
    """
    Returns a dict mapping table_name -> (fieldnames, rows).
    """
    country_by_idx = {i: c for i, c in enumerate(COUNTRIES)}
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

    bank_country_seen: set[tuple] = set()
    account_bank_seen: set[tuple] = set()
    account_country_seen: set[tuple] = set()

    next_bank_id    = 1001
    next_account_id = 1
    next_transfer_id = 1
    next_edge_id    = 1
    next_alert_pk   = 1

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, rec in enumerate(reader):
            if idx >= max_rows:
                break

            from_bank      = rec["from_bank"].strip()
            from_account   = rec["from_account"].strip()
            to_bank        = rec["to_bank"].strip()
            to_account     = rec["to_account"].strip()
            amount         = float(rec["amount_paid"].strip() or "0")
            currency       = rec["payment_currency"].strip() or "USD"
            ts             = rec["timestamp"].strip() or "1970-01-01T00:00:00Z"
            laundering     = rec.get("is_laundering", "0").strip() or "0"
            tx_id          = rec.get("transaction_id", "").strip() or str(next_transfer_id)
            payment_format = rec.get("payment_format", "UNKNOWN").strip() or "UNKNOWN"

            for bank_key in (from_bank, to_bank):
                if bank_key not in banks:
                    bank_id = next_bank_id
                    next_bank_id += 1
                    banks[bank_key] = bank_id
                    cc_idx = abs(_java_hashcode(bank_key)) % len(COUNTRIES)
                    cc = COUNTRIES[cc_idx]
                    bank_rows.append((bank_id, bank_key, f"Bank-{bank_key}", cc[1]))
                    pair = (bank_id, cc[0])
                    if pair not in bank_country_seen:
                        bank_country_seen.add(pair)
                        bank_country_rows.append((next_edge_id, bank_id, cc[0], "true"))
                        next_edge_id += 1

            from_bank_id = banks[from_bank]
            to_bank_id   = banks[to_bank]

            for acct_key, acct_num, bank_id_val in (
                (f"{from_bank}:{from_account}", from_account, from_bank_id),
                (f"{to_bank}:{to_account}",   to_account,   to_bank_id),
            ):
                if acct_key not in accounts:
                    account_id = next_account_id
                    next_account_id += 1
                    accounts[acct_key] = account_id
                    h = abs(_java_hashcode(acct_key))
                    bucket = h % 100
                    if bucket < 5:
                        risk_score = round(0.85 + (h % 15) / 100.0, 2)
                        is_blocked = "true"
                    elif bucket < 20:
                        risk_score = round(0.71 + (h % 14) / 100.0, 2)
                        is_blocked = "false"
                    else:
                        risk_score = round(0.05 + (h % 65) / 100.0, 2)
                        is_blocked = "false"
                    # ~30% of accounts have no openedDate (NULL) — mirrors the H2 seed and
                    # makes hasNot('openedDate') return non-zero results in demos.
                    # Use a deterministic hash so the same account always has/lacks a date.
                    has_opened_date = (h * 11) % 100 >= 30
                    opened_date = (ts[:10].replace("/", "-") if ts else "2020-01-01") if has_opened_date else None
                    acct_type = "BUSINESS" if h % 3 == 0 else "PERSONAL"
                    account_rows.append(
                        (account_id, acct_num, acct_key.split(":", 1)[0],
                         risk_score, is_blocked, opened_date, acct_type)
                    )
                    pair = (account_id, bank_id_val)
                    if pair not in account_bank_seen:
                        account_bank_seen.add(pair)
                        account_bank_rows.append((next_edge_id, account_id, bank_id_val, "true"))
                        next_edge_id += 1

            from_pk = accounts[f"{from_bank}:{from_account}"]
            to_pk   = accounts[f"{to_bank}:{to_account}"]

            transfer_rows.append(
                (next_transfer_id, from_pk, to_pk,
                 tx_id, amount, currency, payment_format, ts, laundering)
            )
            next_transfer_id += 1

            to_cc_idx   = abs(_java_hashcode(to_bank)) % len(COUNTRIES)
            to_cid      = COUNTRIES[to_cc_idx][0]
            pair = (from_pk, to_cid)
            if pair not in account_country_seen:
                account_country_seen.add(pair)
                account_country_rows.append((next_edge_id, from_pk, to_cid, payment_format, ts))
                next_edge_id += 1

            if laundering == "1":
                severity = "HIGH" if amount > 50000 else "MEDIUM"
                alert_pk = next_alert_pk + 500000
                next_alert_pk += 1
                alert_rows.append(
                    (alert_pk, f"ALERT-{tx_id}", "SUSPICIOUS_TRANSFER", severity, "OPEN", ts)
                )
                account_alert_rows.append(
                    (next_edge_id, from_pk, alert_pk, ts, "Suspicious outbound transfer")
                )
                next_edge_id += 1

    country_rows = [(c[0], c[1], c[2], c[3], c[4], c[5]) for c in COUNTRIES]

    return {
        "countries":      (["id", "country_code", "country_name", "risk_level", "region", "fatf_blacklist"],   country_rows),
        "banks":          (["id", "bank_id", "bank_name", "country_code"],                                      bank_rows),
        "accounts":       (["id", "account_id", "bank_id", "risk_score", "is_blocked", "opened_date", "account_type"], account_rows),
        "transfers":      (["id", "out_id", "in_id", "transaction_id", "amount", "currency", "payment_format", "event_time", "is_laundering"], transfer_rows),
        "bank_country":   (["id", "out_id", "in_id", "is_headquarters"],                                        bank_country_rows),
        "account_bank":   (["id", "out_id", "in_id", "is_primary"],                                             account_bank_rows),
        "alerts":         (["id", "alert_id", "alert_type", "severity", "status", "raised_at"],                 alert_rows),
        "account_country":(["id", "out_id", "in_id", "channel_type", "routed_at"],                              account_country_rows),
        "account_alert":  (["id", "out_id", "in_id", "flagged_at", "reason"],                                   account_alert_rows),
    }


# ── Social Network data builder ──────────────────────────────────────────────

def build_social_network_tables() -> dict[str, tuple[list, list]]:
    """Return (fieldnames, rows) for every social-network table."""
    persons = [
        (1, "Alice",   29, "alice@example.com",   "San Francisco", "USA",  "2018-03-15"),
        (2, "Bob",     34, "bob@example.com",     "New York",      "USA",  "2016-07-22"),
        (3, "Carol",   27, "carol@example.com",   "London",        "UK",   "2020-01-10"),
        (4, "Dave",    41, "dave@example.com",    "Berlin",        "DE",   "2015-09-05"),
        (5, "Eve",     25, "eve@example.com",     "Paris",         "FR",   "2021-06-30"),
        (6, "Frank",   38, "frank@example.com",   "Tokyo",         "JP",   "2014-11-18"),
        (7, "Grace",   32, "grace@example.com",   "Sydney",        "AU",   "2019-04-02"),
        (8, "Hank",    45, "hank@example.com",    "Toronto",       "CA",   "2013-08-28"),
        (9, "Iris",    30, "iris@example.com",    "Singapore",     "SG",   "2017-12-14"),
        (10,"Jake",    22, "jake@example.com",    "Amsterdam",     "NL",   "2022-02-07"),
    ]
    companies = [
        (1, "TechCorp",   "Technology",    "USA", 2008, 5000),
        (2, "FinanceHub", "Finance",       "UK",  2001, 2500),
        (3, "DataStream", "Data Analytics","DE",  2015,  800),
        (4, "CloudNine",  "Cloud Services","USA", 2012, 3200),
        (5, "AIWorks",    "AI/ML",         "SG",  2018,  450),
    ]
    cities = [
        (1, "San Francisco", "USA", 883305,   "America/Los_Angeles"),
        (2, "New York",      "USA", 8336817,  "America/New_York"),
        (3, "London",        "UK",  8982000,  "Europe/London"),
        (4, "Berlin",        "DE",  3769495,  "Europe/Berlin"),
        (5, "Paris",         "FR",  2161000,  "Europe/Paris"),
        (6, "Tokyo",         "JP",  13960000, "Asia/Tokyo"),
        (7, "Sydney",        "AU",  5312000,  "Australia/Sydney"),
        (8, "Toronto",       "CA",  2930000,  "America/Toronto"),
        (9, "Singapore",     "SG",  5850000,  "Asia/Singapore"),
        (10,"Amsterdam",     "NL",  921402,   "Europe/Amsterdam"),
    ]
    skills = [
        (1, "Python",           "Programming"),
        (2, "Java",             "Programming"),
        (3, "SQL",              "Data"),
        (4, "Machine Learning", "AI/ML"),
        (5, "Kubernetes",       "DevOps"),
        (6, "React",            "Frontend"),
        (7, "Graph Databases",  "Data"),
        (8, "Spark",            "Data Engineering"),
    ]
    knows = [
        (1,  1, 2, "2019-01-01", "strong"),
        (2,  1, 3, "2020-06-15", "moderate"),
        (3,  2, 4, "2018-03-10", "strong"),
        (4,  3, 5, "2021-02-28", "moderate"),
        (5,  4, 6, "2016-09-01", "weak"),
        (6,  5, 7, "2022-01-05", "strong"),
        (7,  6, 8, "2015-07-20", "moderate"),
        (8,  7, 9, "2020-11-11", "strong"),
        (9,  8, 10,"2014-04-04", "weak"),
        (10, 1, 9, "2021-08-08", "moderate"),
    ]
    works_at = [
        (1, 1, 1, "Software Engineer",   2020),
        (2, 2, 2, "Data Analyst",        2018),
        (3, 3, 3, "ML Engineer",         2021),
        (4, 4, 1, "Engineering Manager", 2017),
        (5, 5, 4, "Backend Developer",   2022),
        (6, 6, 5, "AI Researcher",       2019),
        (7, 7, 2, "Financial Analyst",   2020),
        (8, 8, 3, "Data Scientist",      2016),
        (9, 9, 4, "Cloud Architect",     2018),
        (10,10,5, "Junior Developer",    2022),
    ]
    lives_in = [
        (1, 1, 1, "2018-01-01"), (2, 2, 2, "2016-01-01"),
        (3, 3, 3, "2020-01-01"), (4, 4, 4, "2015-01-01"),
        (5, 5, 5, "2021-01-01"), (6, 6, 6, "2014-01-01"),
        (7, 7, 7, "2019-01-01"), (8, 8, 8, "2013-01-01"),
        (9, 9, 9, "2017-01-01"), (10,10,10,"2022-01-01"),
    ]
    has_skill = [
        (1, 1, 1, "Expert",       5), (2, 1, 4, "Intermediate", 2),
        (3, 2, 3, "Expert",       8), (4, 2, 7, "Intermediate", 3),
        (5, 3, 1, "Intermediate", 3), (6, 3, 4, "Expert",       4),
        (7, 4, 2, "Expert",      10), (8, 4, 5, "Expert",       7),
        (9, 5, 6, "Intermediate", 2),(10, 6, 4, "Expert",       6),
        (11,7, 3, "Intermediate", 5),(12, 8, 8, "Expert",       8),
        (13,9, 5, "Expert",       6),(14,10, 1, "Beginner",     1),
        (15,10,6, "Beginner",     1),
    ]
    company_city = [
        (1,1,1),(2,2,3),(3,3,4),(4,4,2),(5,5,9),
    ]
    return {
        "persons":      (["id","name","age","email","city","country","joined_at"],          persons),
        "companies":    (["id","name","industry","country","founded","employees"],           companies),
        "cities":       (["id","name","country","population","timezone"],                   cities),
        "skills":       (["id","name","category"],                                          skills),
        "knows":        (["id","person_a_id","person_b_id","since","strength"],             knows),
        "works_at":     (["id","person_id","company_id","role","start_year"],               works_at),
        "lives_in":     (["id","person_id","city_id","since"],                              lives_in),
        "has_skill":    (["id","person_id","skill_id","level","years_of_exp"],              has_skill),
        "company_city": (["id","company_id","city_id"],                                    company_city),
    }


# ── DDL for each schema ──────────────────────────────────────────────────────

AML_DDL = {
    "countries":      "CREATE TABLE IF NOT EXISTS iceberg.aml.countries (id BIGINT, country_code VARCHAR, country_name VARCHAR, risk_level VARCHAR, region VARCHAR, fatf_blacklist VARCHAR) WITH (format='PARQUET')",
    "banks":          "CREATE TABLE IF NOT EXISTS iceberg.aml.banks (id BIGINT, bank_id VARCHAR, bank_name VARCHAR, country_code VARCHAR) WITH (format='PARQUET')",
    "accounts":       "CREATE TABLE IF NOT EXISTS iceberg.aml.accounts (id BIGINT, account_id VARCHAR, bank_id VARCHAR, risk_score DOUBLE, is_blocked VARCHAR, opened_date VARCHAR, account_type VARCHAR) WITH (format='PARQUET')",
    "transfers":      "CREATE TABLE IF NOT EXISTS iceberg.aml.transfers (id BIGINT, out_id BIGINT, in_id BIGINT, transaction_id VARCHAR, amount DOUBLE, currency VARCHAR, payment_format VARCHAR, event_time VARCHAR, is_laundering VARCHAR) WITH (format='PARQUET')",
    "bank_country":   "CREATE TABLE IF NOT EXISTS iceberg.aml.bank_country (id BIGINT, out_id BIGINT, in_id BIGINT, is_headquarters VARCHAR) WITH (format='PARQUET')",
    "account_bank":   "CREATE TABLE IF NOT EXISTS iceberg.aml.account_bank (id BIGINT, out_id BIGINT, in_id BIGINT, is_primary VARCHAR) WITH (format='PARQUET')",
    "alerts":         "CREATE TABLE IF NOT EXISTS iceberg.aml.alerts (id BIGINT, alert_id VARCHAR, alert_type VARCHAR, severity VARCHAR, status VARCHAR, raised_at VARCHAR) WITH (format='PARQUET')",
    "account_country":"CREATE TABLE IF NOT EXISTS iceberg.aml.account_country (id BIGINT, out_id BIGINT, in_id BIGINT, channel_type VARCHAR, routed_at VARCHAR) WITH (format='PARQUET')",
    "account_alert":  "CREATE TABLE IF NOT EXISTS iceberg.aml.account_alert (id BIGINT, out_id BIGINT, in_id BIGINT, flagged_at VARCHAR, reason VARCHAR) WITH (format='PARQUET')",
}

SOCIAL_DDL = {
    "persons":      "CREATE TABLE IF NOT EXISTS iceberg.social_network.persons (id BIGINT, name VARCHAR, age INTEGER, email VARCHAR, city VARCHAR, country VARCHAR, joined_at VARCHAR) WITH (format='PARQUET')",
    "companies":    "CREATE TABLE IF NOT EXISTS iceberg.social_network.companies (id BIGINT, name VARCHAR, industry VARCHAR, country VARCHAR, founded INTEGER, employees INTEGER) WITH (format='PARQUET')",
    "cities":       "CREATE TABLE IF NOT EXISTS iceberg.social_network.cities (id BIGINT, name VARCHAR, country VARCHAR, population BIGINT, timezone VARCHAR) WITH (format='PARQUET')",
    "skills":       "CREATE TABLE IF NOT EXISTS iceberg.social_network.skills (id BIGINT, name VARCHAR, category VARCHAR) WITH (format='PARQUET')",
    "knows":        "CREATE TABLE IF NOT EXISTS iceberg.social_network.knows (id BIGINT, person_a_id BIGINT, person_b_id BIGINT, since VARCHAR, strength VARCHAR) WITH (format='PARQUET')",
    "works_at":     "CREATE TABLE IF NOT EXISTS iceberg.social_network.works_at (id BIGINT, person_id BIGINT, company_id BIGINT, role VARCHAR, start_year INTEGER) WITH (format='PARQUET')",
    "lives_in":     "CREATE TABLE IF NOT EXISTS iceberg.social_network.lives_in (id BIGINT, person_id BIGINT, city_id BIGINT, since VARCHAR) WITH (format='PARQUET')",
    "has_skill":    "CREATE TABLE IF NOT EXISTS iceberg.social_network.has_skill (id BIGINT, person_id BIGINT, skill_id BIGINT, level VARCHAR, years_of_exp INTEGER) WITH (format='PARQUET')",
    "company_city": "CREATE TABLE IF NOT EXISTS iceberg.social_network.company_city (id BIGINT, company_id BIGINT, city_id BIGINT) WITH (format='PARQUET')",
}


# ── Hive CSV table helpers ───────────────────────────────────────────────────

def _hive_csv_ddl(tmp_table: str, schema_path: str, columns: list[str], table_cols_sql: str) -> str:
    """
    Create a Hive external table that reads a CSV from MinIO.
    schema_path is like 's3://warehouse/csv_staging/aml/accounts/'.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS {tmp_table} ({table_cols_sql}) "
        f"WITH ("
        f"  format = 'CSV', "
        f"  skip_header_line_count = 1, "
        f"  external_location = '{schema_path}'"
        f")"
    )


def _col_type(col: str) -> str:
    """Infer a Hive/Trino VARCHAR type for each column (all read as VARCHAR from CSV)."""
    return "VARCHAR"


def load_tables_via_csv(
    schema: str,
    tables: dict[str, tuple[list, list]],
    ddl_map: dict[str, str],
    wipe: bool,
    hive_schema: str = "hive.csv_staging",
    bucket: str = MINIO_BUCKET,
    prefix: str = "csv_staging",
) -> None:
    """
    For each table:
      1. Write CSV bytes and upload to MinIO at s3://<bucket>/<prefix>/<schema>/<table>/data.csv
      2. Create a Hive external CSV table pointing at that path
      3. INSERT INTO iceberg.<schema>.<table> SELECT (casted cols) FROM hive_csv_table
      4. Drop the Hive external table

    This is one INSERT per table instead of one INSERT per row.
    """
    # Ensure staging schema exists in Hive
    trino_exec(f"CREATE SCHEMA IF NOT EXISTS {hive_schema}")

    for table_name, (fieldnames, rows) in tables.items():
        iceberg_table = f"iceberg.{schema}.{table_name}"
        hive_tmp      = f"{hive_schema}.{schema}_{table_name}_csv"
        s3_path       = f"s3://{bucket}/{prefix}/{schema}/{table_name}/"

        print(f"\n  [{table_name}] {len(rows):,} rows …")

        # 1. Upload CSV to MinIO
        csv_bytes = _write_csv_bytes(fieldnames, rows)
        upload_csv(csv_bytes, bucket, f"{prefix}/{schema}/{table_name}/data.csv")

        # 2. Decide whether to drop-and-recreate or skip.
        #    Always DROP+CREATE (not just DELETE) so a stale schema from a
        #    previous load never causes TYPE_MISMATCH errors on INSERT.
        new_row_count = len(rows)
        cnt = trino_count(f"SELECT COUNT(*) FROM {iceberg_table}")

        need_reload = False
        if wipe:
            need_reload = True
            print(f"  Wiping {iceberg_table} (DROP + recreate) …")
        elif cnt < 0:
            # Table does not exist yet
            need_reload = True
        elif cnt >= new_row_count:
            # Extra sanity check for the accounts table: if opened_date has zero NULLs
            # but the in-memory rows contain NULLs, the table was loaded by an older
            # version of the seeder that serialised None as '' without NULLIF.
            # Force a reload so hasNot('openedDate') returns correct results.
            if table_name == "accounts" and "opened_date" in fieldnames:
                null_count_in_memory = sum(1 for row in rows if row[fieldnames.index("opened_date")] is None)
                if null_count_in_memory > 0:
                    null_count_in_db = trino_count(
                        f"SELECT COUNT(*) FROM {iceberg_table} WHERE opened_date IS NULL"
                    )
                    if null_count_in_db == 0:
                        print(f"  {iceberg_table} has {cnt:,} rows but opened_date has 0 NULLs "
                              f"(expected ~{null_count_in_memory:,}) — reloading to fix NULL values …")
                        need_reload = True
            if not need_reload:
                print(f"  {iceberg_table} already has {cnt:,} rows "
                      f"(≥ {new_row_count:,} to insert) — skipping. Use --wipe to reload.")
                continue
        else:
            need_reload = True
            print(f"  {iceberg_table} has {cnt:,} rows but {new_row_count:,} to insert "
                  f"— dropping and reloading …")

        if need_reload:
            # DROP ensures we pick up the correct column types from ddl_map,
            # avoiding TYPE_MISMATCH when the table was created by an older version.
            trino_exec(f"DROP TABLE IF EXISTS {iceberg_table}")
            trino_exec(ddl_map[table_name])

        # 3. Create Hive external CSV table
        cols_varchar = ", ".join(f"{c} VARCHAR" for c in fieldnames)
        create_hive = (
            f"CREATE TABLE IF NOT EXISTS {hive_tmp} ({cols_varchar}) "
            f"WITH (format = 'CSV', skip_header_line_count = 1, "
            f"external_location = '{s3_path}')"
        )
        trino_exec(f"DROP TABLE IF EXISTS {hive_tmp}")
        trino_exec(create_hive)

        # 4. INSERT SELECT with type casts
        # The Hive CSV table reads every column as VARCHAR.
        # We cast each column to the native type declared in the Iceberg DDL.
        #
        # Surrogate-key columns (always BIGINT):
        #   id, out_id, in_id  — and *_id cols that are FK references to other tables
        #
        # Natural-key label columns (VARCHAR even though they end in _id):
        #   bank_id    in the `banks` vertex table      → "010", "03208" etc.
        #   account_id in the `accounts` vertex table   → "8000EBD30" etc.
        #   alert_id   in the `alerts` vertex table     → "ALERT-123" etc.
        #
        # For edge tables (transfers, account_bank, bank_country, …) these same
        # column names are FK references → BIGINT.

        # Columns that are ALWAYS numeric surrogate keys regardless of table
        _ALWAYS_BIGINT = {
            "id", "out_id", "in_id",
            "from_account_id", "to_account_id",
            "person_a_id", "person_b_id", "person_id",
            "company_id", "city_id", "skill_id",
            "country_id", "transfer_id",
        }
        # Natural-key label columns that are VARCHAR in specific tables.
        # Key = column name, Value = set of table names where it is VARCHAR.
        # In all other tables it is a BIGINT FK.
        _VARCHAR_IN_TABLES: dict[str, set[str]] = {
            "bank_id":    {"banks", "accounts"},   # "010", "03208" etc. in both vertex tables
            "account_id": {"accounts"},             # "8000EBD30" etc.
            "alert_id":   {"alerts"},               # "ALERT-123" etc.
        }

        # Columns that should be treated as NULL when the CSV cell is empty.
        # These are VARCHAR columns that the Python builder sets to None (→ empty
        # string in CSV) to represent a missing / optional value.
        _NULLABLE_VARCHAR_COLS = {"opened_date"}

        def _cast(col: str) -> str:
            """Cast VARCHAR → native type matching the Iceberg DDL."""
            if col in _ALWAYS_BIGINT:
                return f"TRY_CAST({col} AS BIGINT)"
            if col in _VARCHAR_IN_TABLES:
                if table_name in _VARCHAR_IN_TABLES[col]:
                    # VARCHAR in this table; still convert empty → NULL
                    return f"NULLIF({col}, '')" if col in _NULLABLE_VARCHAR_COLS else col
                return f"TRY_CAST({col} AS BIGINT)"
            if col in ("amount", "risk_score"):
                return f"TRY_CAST({col} AS DOUBLE)"
            if col in ("age", "founded", "employees", "start_year",
                       "years_of_exp", "population"):
                return f"TRY_CAST({col} AS INTEGER)"
            # Nullable VARCHAR: convert empty string → NULL so IS NULL queries work
            if col in _NULLABLE_VARCHAR_COLS:
                return f"NULLIF({col}, '')"
            return col  # keep as VARCHAR

        cast_select = ", ".join(_cast(c) for c in fieldnames)
        insert_sql = (
            f"INSERT INTO {iceberg_table} ({', '.join(fieldnames)}) "
            f"SELECT {cast_select} FROM {hive_tmp}"
        )
        print(f"  Inserting via SELECT from Hive CSV table …", end=" ", flush=True)
        t0 = time.time()
        trino_exec(insert_sql)
        elapsed = time.time() - t0
        print(f"done ({elapsed:.1f}s)")

        # 5. Drop the temporary Hive table
        trino_exec(f"DROP TABLE IF EXISTS {hive_tmp}")

        # Verify
        final = trino_count(f"SELECT COUNT(*) FROM {iceberg_table}")
        print(f"  ✓ {iceberg_table} — {final:,} rows")


# ── Main entry-points ─────────────────────────────────────────────────────────

def seed_aml(csv_path: Path, max_rows: int, wipe: bool) -> None:
    print(f"\n=== AML Iceberg seed (CSV bulk load) ===")
    print(f"Source CSV : {csv_path}  (max {max_rows:,} rows)")

    trino_exec("CREATE SCHEMA IF NOT EXISTS iceberg.aml WITH (location = 's3://warehouse/aml')")

    print("\nBuilding in-memory tables from CSV …", end=" ", flush=True)
    tables = build_aml_tables(csv_path, max_rows)
    total_rows = sum(len(r) for _, r in tables.values())
    print(f"done ({total_rows:,} total rows across {len(tables)} tables)")

    load_tables_via_csv("aml", tables, AML_DDL, wipe)
    print("\n✓ AML Iceberg seed complete.")


def seed_social_network(wipe: bool) -> None:
    print("\n=== Social Network Iceberg seed (CSV bulk load) ===")

    trino_exec("CREATE SCHEMA IF NOT EXISTS iceberg.social_network WITH (location = 's3://warehouse/social_network')")

    tables = build_social_network_tables()
    load_tables_via_csv("social_network", tables, SOCIAL_DDL, wipe)
    print("\n✓ Social Network Iceberg seed complete.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def _repo_root() -> Path:
    for p in [Path.cwd()] + list(Path.cwd().parents):
        if (p / "pom.xml").exists():
            return p
    return Path.cwd()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bulk-load graph data into Iceberg via MinIO CSV upload + Trino INSERT SELECT"
    )
    parser.add_argument(
        "--dataset",
        choices=["aml", "social_network", "all"],
        default="aml",
        help="Which dataset to seed (default: aml)",
    )
    parser.add_argument(
        "--csv",
        default=None,
        help="Path to aml-demo.csv (AML dataset only; defaults to data/aml-demo.csv in repo root)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=100_000,
        help="Maximum CSV rows to ingest (AML dataset only; default: 100 000)",
    )
    parser.add_argument(
        "--wipe",
        action="store_true",
        help="Delete existing rows before loading",
    )
    args = parser.parse_args()

    repo = _repo_root()

    if args.dataset in ("aml", "all"):
        csv_path = Path(args.csv) if args.csv else repo / "data" / "aml-demo.csv"
        if not csv_path.exists():
            print(f"ERROR: CSV not found: {csv_path}", file=sys.stderr)
            sys.exit(1)
        seed_aml(csv_path, args.rows, args.wipe)

    if args.dataset in ("social_network", "all"):
        seed_social_network(args.wipe)


if __name__ == "__main__":
    main()

