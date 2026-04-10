#!/usr/bin/env python3
"""
seed_aml_h2.py
--------------
Seeds the AML demo graph into H2 by reading the CSV and loading data in bulk.

Two load modes (controlled by --mode flag):
  csvread  (default) — fastest: writes intermediate CSVs to a temp dir and
                       loads them into H2 with a single CSVREAD() call per
                       table.  No per-row SQL; all I/O is file-based.
  sql      (legacy)  — generates MERGE INTO … statements and runs them via
                       H2 RunScript.  Retained for environments where the
                       intermediate CSV temp dir is not writable.

Usage:
    python3 demo/aml/scripts/seed_aml_h2.py
    python3 demo/aml/scripts/seed_aml_h2.py --wipe
    python3 demo/aml/scripts/seed_aml_h2.py --csv path/to/aml-demo.csv --max-rows 5000
    python3 demo/aml/scripts/seed_aml_h2.py --mode sql   # legacy row-by-row mode

Tables created (matching aml-mapping.json):
    aml_accounts, aml_banks, aml_countries, aml_alerts,
    aml_transfers, aml_account_bank, aml_bank_country,
    aml_account_alert, aml_transfer_channel
"""

from __future__ import annotations

import argparse
import csv
import glob
import hashlib
import locale
import os
import pathlib
import subprocess
import tempfile

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

def _repo_root() -> pathlib.Path:
    for p in [pathlib.Path.cwd()] + list(pathlib.Path.cwd().parents):
        if (p / "pom.xml").exists():
            return p
    return pathlib.Path.cwd()


def _h2_jar() -> str:
    home = pathlib.Path.home()
    jars = sorted(glob.glob(
        str(home / ".m2/repository/com/h2database/h2/**/*.jar"), recursive=True
    ))
    if not jars:
        raise RuntimeError(
            "H2 jar not found in ~/.m2. Run 'mvn test-compile' once to download it."
        )
    return jars[-1]


def _db_url(repo_root: pathlib.Path) -> str:
    db_file = repo_root / "data" / "graph"
    # No AUTO_SERVER — seed runs standalone with the app server stopped.
    return f"jdbc:h2:file:{db_file}"


# ---------------------------------------------------------------------------
# Static reference data (matches the Groovy loader)
# ---------------------------------------------------------------------------

COUNTRIES = [
    ("US",  "United States",  "LOW",    "Americas",    "false"),
    ("GB",  "United Kingdom", "LOW",    "Europe",      "false"),
    ("DE",  "Germany",        "LOW",    "Europe",      "false"),
    ("CH",  "Switzerland",    "MEDIUM", "Europe",      "false"),
    ("HK",  "Hong Kong",      "MEDIUM", "Asia",        "false"),
    ("SG",  "Singapore",      "LOW",    "Asia",        "false"),
    ("AE",  "UAE",            "MEDIUM", "Middle East", "false"),
    ("NG",  "Nigeria",        "HIGH",   "Africa",      "false"),
    ("KY",  "Cayman Islands", "HIGH",   "Americas",    "true"),
    ("PA",  "Panama",         "HIGH",   "Americas",    "true"),
]

# Country code -> id (1-based index)
COUNTRY_ID: dict[str, int] = {c[0]: i + 1 for i, c in enumerate(COUNTRIES)}


def _country_for_bank(bank_id: str) -> str:
    """Deterministically pick a country code for a bank id (mirrors Groovy loader)."""
    idx = int(hashlib.md5(bank_id.encode()).hexdigest(), 16) % len(COUNTRIES)
    return COUNTRIES[idx][0]


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS aml_countries (
    id BIGINT PRIMARY KEY,
    country_code  VARCHAR, country_name VARCHAR,
    risk_level    VARCHAR, region       VARCHAR,
    fatf_blacklist VARCHAR
);
CREATE TABLE IF NOT EXISTS aml_banks (
    id BIGINT PRIMARY KEY,
    bank_id    VARCHAR, bank_name  VARCHAR,
    country_code VARCHAR, swift_code VARCHAR, tier VARCHAR
);
CREATE TABLE IF NOT EXISTS aml_accounts (
    id BIGINT PRIMARY KEY,
    account_id   VARCHAR, bank_id    VARCHAR,
    account_type VARCHAR, risk_score INTEGER,
    is_blocked   VARCHAR, opened_date VARCHAR
);
CREATE TABLE IF NOT EXISTS aml_alerts (
    id BIGINT PRIMARY KEY,
    alert_id VARCHAR, alert_type VARCHAR,
    severity VARCHAR, status     VARCHAR, raised_at VARCHAR
);
CREATE TABLE IF NOT EXISTS aml_transfers (
    id BIGINT PRIMARY KEY,
    from_account_id BIGINT, to_account_id BIGINT,
    transaction_id  VARCHAR, amount        DOUBLE,
    currency        VARCHAR, payment_format VARCHAR,
    event_time      VARCHAR, is_laundering  VARCHAR
);
CREATE TABLE IF NOT EXISTS aml_account_bank (
    id BIGINT PRIMARY KEY,
    account_id BIGINT, bank_id BIGINT,
    since VARCHAR, is_primary VARCHAR
);
CREATE TABLE IF NOT EXISTS aml_bank_country (
    id BIGINT PRIMARY KEY,
    bank_id BIGINT, country_id BIGINT, is_headquarters VARCHAR
);
CREATE TABLE IF NOT EXISTS aml_account_alert (
    id BIGINT PRIMARY KEY,
    account_id BIGINT, alert_id BIGINT,
    flagged_at VARCHAR, reason VARCHAR
);
CREATE TABLE IF NOT EXISTS aml_transfer_channel (
    id BIGINT PRIMARY KEY,
    transfer_id BIGINT, country_id BIGINT,
    channel_type VARCHAR, routed_at VARCHAR
);
"""

WIPE_SQL = """
DELETE FROM aml_transfer_channel;
DELETE FROM aml_account_alert;
DELETE FROM aml_bank_country;
DELETE FROM aml_account_bank;
DELETE FROM aml_transfers;
DELETE FROM aml_alerts;
DELETE FROM aml_accounts;
DELETE FROM aml_banks;
DELETE FROM aml_countries;
"""


# ---------------------------------------------------------------------------
# Helper: run SQL via H2 RunScript (one JVM invocation, reads from file)
# ---------------------------------------------------------------------------

def _run_script_file(script_path: str, db_url: str, h2_jar: str) -> None:
    """Execute a SQL script file using H2 RunScript — single JVM call, no arg-length limits."""
    result = subprocess.run(
        ["java", "-cp", h2_jar, "org.h2.tools.RunScript",
         "-url", db_url, "-user", "SA", "-password", "",
         "-script", script_path, "-continueOnError"],
        capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        raise RuntimeError(f"H2 RunScript failed:\n{result.stderr[-800:]}\n{result.stdout[-400:]}")


def _run_sql(sql: str, db_url: str, h2_jar: str) -> None:
    """Write sql to a temp file then execute via RunScript."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False, encoding="utf-8") as tf:
        tf.write(sql)
        tmp = tf.name
    try:
        _run_script_file(tmp, db_url, h2_jar)
    finally:
        os.unlink(tmp)


def _count_accounts(db_url: str, h2_jar: str) -> int:
    result = subprocess.run(
        ["java", "-cp", h2_jar, "org.h2.tools.Shell",
         "-url", db_url, "-user", "SA", "-password", "",
         "-sql", "SELECT COUNT(*) FROM aml_accounts"],
        capture_output=True, text=True, timeout=15,
    )
    if result.returncode != 0:
        return -1
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.isdigit():
            return int(line)
    return -1


# ---------------------------------------------------------------------------
# CSV bulk-load helpers (fast path — uses H2 CSVREAD)
# ---------------------------------------------------------------------------

def _write_csv(path: pathlib.Path, fieldnames: list[str], rows: list[tuple]) -> None:
    """Write rows to a CSV file that H2's CSVREAD() can consume."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(fieldnames)
        w.writerows(rows)


def _csvread_insert(table: str, csv_file: pathlib.Path, columns: list[str], db_url: str, h2_jar: str) -> None:
    """
    Load a CSV into *table* using a single:
      INSERT INTO <table> (<cols>) SELECT <cols> FROM CSVREAD('<file>')
    H2's CSVREAD reads the file name from a column header, so we build
    a SELECT that maps the CSV columns to the table columns.
    """
    cols = ", ".join(columns)
    # Cast id / numeric columns that may arrive as strings from CSVREAD
    cast_cols = ", ".join(
        f"CAST({c} AS BIGINT) AS {c}" if c in ("id", "account_id", "bank_id",
                                                 "country_id", "transfer_id",
                                                 "alert_id", "from_account_id",
                                                 "to_account_id")
        else f"CAST({c} AS DOUBLE) AS {c}" if c == "amount"
        else f"CAST({c} AS INTEGER) AS {c}" if c == "risk_score"
        else c
        for c in columns
    )
    sql = (
        f"INSERT INTO {table} ({cols}) "
        f"SELECT {cast_cols} "
        f"FROM CSVREAD('{csv_file}');"
    )
    _run_sql(sql, db_url, h2_jar)


def _build_data_tables(csv_path: str, max_rows: int):
    """
    Parse the source CSV once and return dictionaries of rows for each table.
    Same logic as the legacy SQL mode — just returns Python tuples instead of
    SQL strings.
    """
    bank_map: dict[str, int] = {}
    account_map: dict[str, int] = {}
    transfer_rows: list[tuple] = []
    transfer_channel_rows: list[tuple] = []
    bank_seq = [0]
    account_seq = [0]
    transfer_seq = [0]
    alert_seq = [0]

    def next_bank_id(bid: str) -> int:
        if bid not in bank_map:
            bank_seq[0] += 1
            bank_map[bid] = bank_seq[0]
        return bank_map[bid]

    def next_account_id(aid: str) -> int:
        if aid not in account_map:
            account_seq[0] += 1
            account_map[aid] = account_seq[0]
        return account_map[aid]

    rows_loaded = 0
    laundering_accounts: set[int] = set()

    csv_path_resolved = pathlib.Path(csv_path).resolve()
    print(f"Reading CSV: {csv_path_resolved}")

    with open(csv_path_resolved, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if rows_loaded >= max_rows:
                break
            from_bank  = row["from_bank"].strip()
            from_acct  = row["from_account"].strip()
            to_bank    = row["to_bank"].strip()
            to_acct    = row["to_account"].strip()
            amount     = float(row["amount_paid"])
            currency   = row["payment_currency"].strip()
            fmt        = (row.get("payment_format") or "UNKNOWN").strip() or "UNKNOWN"
            ts         = row["timestamp"].strip()
            laundering = row["is_laundering"].strip()
            tx_id      = row.get("transaction_id", str(rows_loaded + 1)).strip()
            channel    = "WIRE" if "wire" in fmt.lower() else "DIGITAL"

            from_bank_id = next_bank_id(from_bank)
            to_bank_id   = next_bank_id(to_bank)
            from_acct_id = next_account_id(f"{from_bank}:{from_acct}")
            to_acct_id   = next_account_id(f"{to_bank}:{to_acct}")

            if laundering == "1":
                laundering_accounts.add(from_acct_id)
                laundering_accounts.add(to_acct_id)

            transfer_seq[0] += 1
            tid = transfer_seq[0]
            transfer_rows.append(
                (tid, from_acct_id, to_acct_id, tx_id, f"{amount:.4f}",
                 currency, fmt, ts, laundering)
            )
            cc  = _country_for_bank(from_bank)
            cid = COUNTRY_ID.get(cc, 1)
            transfer_channel_rows.append((tid, tid, cid, channel, ts))
            rows_loaded += 1

    # Build country rows
    country_rows = [(COUNTRY_ID[c[0]], c[0], c[1], c[2], c[3], c[4]) for c in COUNTRIES]

    # Build bank / bank_country rows
    bank_rows = []
    bank_country_rows = []
    for bank_str, bid in bank_map.items():
        cc  = _country_for_bank(bank_str)
        cid = COUNTRY_ID.get(cc, 1)
        bank_rows.append((bid, bank_str, f"Bank-{bank_str}", cc, f"SWIFT{bid}", "TIER1"))
        bank_country_rows.append((bid, bid, cid, "true"))

    # Build account / account_bank rows
    acct_bank: dict[int, int] = {}
    for key, aid in account_map.items():
        acct_bank[aid] = bank_map[key.split(":")[0]]

    account_rows = []
    account_bank_rows = []
    for key, aid in account_map.items():
        bank_str, acct_str = key.split(":", 1)
        bid     = bank_map[bank_str]
        risk    = 85 if aid in laundering_accounts else 30
        blocked = "true" if aid in laundering_accounts and risk > 80 else "false"
        has_opened_date = (int(hashlib.md5(acct_str.encode()).hexdigest(), 16) * 11) % 100 >= 30
        opened_date_val = "2020-01-01" if has_opened_date else ""
        account_rows.append((aid, acct_str, bank_str, "CHECKING", risk, blocked, opened_date_val))
        account_bank_rows.append((aid, aid, bid, "2020-01-01", "true"))

    # Build alert rows
    alert_rows = []
    account_alert_rows = []
    for i, aid in enumerate(sorted(laundering_accounts), 1):
        alert_seq[0] += 1
        alid = alert_seq[0]
        alert_rows.append((alid, f"ALERT-{alid}", "SUSPICIOUS_TRANSFER", "HIGH", "OPEN", "2022-09-01"))
        account_alert_rows.append((alid, aid, alid, "2022-09-01", "Laundering flag"))

    return dict(
        rows_loaded=rows_loaded,
        countries=country_rows,
        banks=bank_rows,
        accounts=account_rows,
        transfers=transfer_rows,
        bank_country=bank_country_rows,
        account_bank=account_bank_rows,
        alerts=alert_rows,
        account_alert=account_alert_rows,
        transfer_channel=transfer_channel_rows,
    )


def seed_csv_bulk(csv_path: str, max_rows: int = 100_000, wipe: bool = False) -> dict:
    """
    Fast-path seeder: parse CSV once → write per-table CSV files to a temp
    directory → load each table with a single H2 CSVREAD() INSERT statement.

    This is ~10-50x faster than the legacy row-by-row MERGE approach for
    large datasets because H2 processes the CSV entirely in-JVM without
    re-spawning for each row.
    """
    repo_root = _repo_root()
    h2_jar    = _h2_jar()
    db_url    = _db_url(repo_root)

    if not wipe:
        count = _count_accounts(db_url, h2_jar)
        if count > 0:
            # Only skip if we already have at least as many accounts as the requested load.
            # A rough proxy: 500 k transactions ≈ 50 k accounts; use count >= max_rows // 10.
            # More precisely: re-seed if the existing account count is less than 80 % of what
            # a fresh load of max_rows would produce (accounts ≈ max_rows * 0.14 for HI-Small).
            # Simplest safe rule: skip only when count >= max_rows * 0.10.
            threshold = max(1, int(max_rows * 0.10))
            if count >= threshold:
                print(f"H2 already contains {count:,} account(s) (≥ threshold {threshold:,} for "
                      f"{max_rows:,} requested rows) — skipping. Use --wipe to reload.")
                return {"accountsCreated": count, "skipped": True}
            print(f"H2 has only {count:,} account(s) but {max_rows:,} rows requested "
                  f"(threshold {threshold:,}) — reloading with wipe …")
            wipe = True   # fall through to wipe + reload

    # 1. Create tables
    _run_sql(DDL, db_url, h2_jar)

    if wipe:
        print("Wiping existing data …")
        _run_sql(WIPE_SQL, db_url, h2_jar)

    # 2. Parse CSV and collect rows
    data = _build_data_tables(csv_path, max_rows)

    # 3. Write per-table CSVs and bulk-load via CSVREAD
    with tempfile.TemporaryDirectory(prefix="aml_h2_seed_") as tmpdir:
        tmp = pathlib.Path(tmpdir)
        print(f"Writing intermediate CSVs to {tmpdir} …")

        # Countries
        countries_csv = tmp / "countries.csv"
        _write_csv(countries_csv,
                   ["id", "country_code", "country_name", "risk_level", "region", "fatf_blacklist"],
                   data["countries"])
        _csvread_insert("aml_countries", countries_csv,
                        ["id", "country_code", "country_name", "risk_level", "region", "fatf_blacklist"],
                        db_url, h2_jar)

        # Banks
        banks_csv = tmp / "banks.csv"
        _write_csv(banks_csv,
                   ["id", "bank_id", "bank_name", "country_code", "swift_code", "tier"],
                   data["banks"])
        _csvread_insert("aml_banks", banks_csv,
                        ["id", "bank_id", "bank_name", "country_code", "swift_code", "tier"],
                        db_url, h2_jar)

        # Accounts
        accounts_csv = tmp / "accounts.csv"
        _write_csv(accounts_csv,
                   ["id", "account_id", "bank_id", "account_type", "risk_score", "is_blocked", "opened_date"],
                   data["accounts"])
        _csvread_insert("aml_accounts", accounts_csv,
                        ["id", "account_id", "bank_id", "account_type", "risk_score", "is_blocked", "opened_date"],
                        db_url, h2_jar)

        # Transfers
        transfers_csv = tmp / "transfers.csv"
        _write_csv(transfers_csv,
                   ["id", "from_account_id", "to_account_id", "transaction_id",
                    "amount", "currency", "payment_format", "event_time", "is_laundering"],
                   data["transfers"])
        _csvread_insert("aml_transfers", transfers_csv,
                        ["id", "from_account_id", "to_account_id", "transaction_id",
                         "amount", "currency", "payment_format", "event_time", "is_laundering"],
                        db_url, h2_jar)

        # account_bank
        account_bank_csv = tmp / "account_bank.csv"
        _write_csv(account_bank_csv,
                   ["id", "account_id", "bank_id", "since", "is_primary"],
                   data["account_bank"])
        _csvread_insert("aml_account_bank", account_bank_csv,
                        ["id", "account_id", "bank_id", "since", "is_primary"],
                        db_url, h2_jar)

        # bank_country
        bank_country_csv = tmp / "bank_country.csv"
        _write_csv(bank_country_csv,
                   ["id", "bank_id", "country_id", "is_headquarters"],
                   data["bank_country"])
        _csvread_insert("aml_bank_country", bank_country_csv,
                        ["id", "bank_id", "country_id", "is_headquarters"],
                        db_url, h2_jar)

        # Alerts
        if data["alerts"]:
            alerts_csv = tmp / "alerts.csv"
            _write_csv(alerts_csv,
                       ["id", "alert_id", "alert_type", "severity", "status", "raised_at"],
                       data["alerts"])
            _csvread_insert("aml_alerts", alerts_csv,
                            ["id", "alert_id", "alert_type", "severity", "status", "raised_at"],
                            db_url, h2_jar)

        # account_alert
        if data["account_alert"]:
            account_alert_csv = tmp / "account_alert.csv"
            _write_csv(account_alert_csv,
                       ["id", "account_id", "alert_id", "flagged_at", "reason"],
                       data["account_alert"])
            _csvread_insert("aml_account_alert", account_alert_csv,
                            ["id", "account_id", "alert_id", "flagged_at", "reason"],
                            db_url, h2_jar)

        # transfer_channel
        transfer_channel_csv = tmp / "transfer_channel.csv"
        _write_csv(transfer_channel_csv,
                   ["id", "transfer_id", "country_id", "channel_type", "routed_at"],
                   data["transfer_channel"])
        _csvread_insert("aml_transfer_channel", transfer_channel_csv,
                        ["id", "transfer_id", "country_id", "channel_type", "routed_at"],
                        db_url, h2_jar)

        print("All CSVs loaded via CSVREAD() — temp files cleaned up.")

    stats = {
        "rowsLoaded":       data["rows_loaded"],
        "accountsCreated":  len(data["accounts"]),
        "banksCreated":     len(data["banks"]),
        "transfersCreated": len(data["transfers"]),
        "alertsCreated":    len(data["alerts"]),
    }
    print(f"\n✓ AML H2 bulk-CSV seed complete: {stats}")
    return stats


# ---------------------------------------------------------------------------
# Main seed function
# ---------------------------------------------------------------------------

def seed(csv_path: str, max_rows: int = 100_000, wipe: bool = False, mode: str = "csvread") -> dict:
    """Seed AML data into H2.

    mode='csvread'  — fast bulk load via H2 CSVREAD() (default)
    mode='sql'      — legacy row-by-row MERGE INTO statements
    """
    if mode == "csvread":
        return seed_csv_bulk(csv_path, max_rows=max_rows, wipe=wipe)
    return _seed_sql(csv_path, max_rows=max_rows, wipe=wipe)


def _seed_sql(csv_path: str, max_rows: int = 100_000, wipe: bool = False) -> dict:
    repo_root = _repo_root()
    h2_jar    = _h2_jar()
    db_url    = _db_url(repo_root)

    # Skip if already seeded (uses Shell -sql for a single query — still one JVM)
    if not wipe:
        count = _count_accounts(db_url, h2_jar)
        if count > 0:
            threshold = max(1, int(max_rows * 0.10))
            if count >= threshold:
                print(f"H2 already contains {count:,} account(s) (≥ threshold {threshold:,} for "
                      f"{max_rows:,} requested rows) — skipping. Use --wipe to reload.")
                return {"accountsCreated": count, "skipped": True}
            print(f"H2 has only {count:,} account(s) but {max_rows:,} rows requested "
                  f"(threshold {threshold:,}) — reloading with wipe …")
            wipe = True

    # ── Collect ALL SQL into one big script ───────────────────────────────
    all_sql: list[str] = []

    all_sql.append(DDL)
    if wipe:
        print("Will wipe existing data …")
        all_sql.append(WIPE_SQL)

    # ── Insert countries ──────────────────────────────────────────────────
    for c in COUNTRIES:
        all_sql.append(
            f"MERGE INTO aml_countries KEY(id) VALUES ({COUNTRY_ID[c[0]]},'{c[0]}','{c[1]}','{c[2]}','{c[3]}','{c[4]}');"
        )

    # ── Read CSV and build INSERT rows ────────────────────────────────────
    bank_map: dict[str, int]    = {}
    account_map: dict[str, int] = {}
    transfer_rows: list[tuple]  = []
    transfer_channel_rows: list[tuple] = []

    bank_seq    = [0]
    account_seq = [0]
    transfer_seq = [0]
    alert_seq   = [0]

    def next_bank_id(bid: str) -> int:
        if bid not in bank_map:
            bank_seq[0] += 1
            bank_map[bid] = bank_seq[0]
        return bank_map[bid]

    def next_account_id(aid: str) -> int:
        if aid not in account_map:
            account_seq[0] += 1
            account_map[aid] = account_seq[0]
        return account_map[aid]

    rows_loaded = 0
    laundering_accounts: set[int] = set()

    csv_path_resolved = pathlib.Path(csv_path).resolve()
    print(f"Reading CSV: {csv_path_resolved}")

    with open(csv_path_resolved, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if rows_loaded >= max_rows:
                break
            from_bank  = row["from_bank"].strip()
            from_acct  = row["from_account"].strip()
            to_bank    = row["to_bank"].strip()
            to_acct    = row["to_account"].strip()
            amount     = float(row["amount_paid"])
            currency   = row["payment_currency"].strip()
            fmt        = (row.get("payment_format") or "UNKNOWN").strip() or "UNKNOWN"
            ts         = row["timestamp"].strip()
            laundering = row["is_laundering"].strip()
            tx_id      = row.get("transaction_id", str(rows_loaded + 1)).strip()
            channel    = "WIRE" if "wire" in fmt.lower() else "DIGITAL"

            from_bank_id = next_bank_id(from_bank)
            to_bank_id   = next_bank_id(to_bank)
            from_acct_id = next_account_id(f"{from_bank}:{from_acct}")
            to_acct_id   = next_account_id(f"{to_bank}:{to_acct}")

            if laundering == "1":
                laundering_accounts.add(from_acct_id)
                laundering_accounts.add(to_acct_id)

            transfer_seq[0] += 1
            tid = transfer_seq[0]
            transfer_rows.append(
                (tid, from_acct_id, to_acct_id, tx_id, f"{amount:.4f}",
                 currency, fmt, ts, laundering)
            )
            cc  = _country_for_bank(from_bank)
            cid = COUNTRY_ID.get(cc, 1)
            transfer_channel_rows.append((tid, tid, cid, channel, ts))
            rows_loaded += 1

    # ── Banks ─────────────────────────────────────────────────────────────
    for bank_str, bid in bank_map.items():
        cc  = _country_for_bank(bank_str)
        cid = COUNTRY_ID.get(cc, 1)
        all_sql.append(f"MERGE INTO aml_banks KEY(id) VALUES ({bid},'{bank_str}','Bank-{bank_str}','{cc}','SWIFT{bid}','TIER1');")
        all_sql.append(f"MERGE INTO aml_bank_country KEY(id) VALUES ({bid},{bid},{cid},'true');")

    # ── Accounts ──────────────────────────────────────────────────────────
    acct_bank: dict[int, int] = {}
    for key, aid in account_map.items():
        acct_bank[aid] = bank_map[key.split(":")[0]]

    for key, aid in account_map.items():
        bank_str, acct_str = key.split(":", 1)
        bid     = bank_map[bank_str]
        risk    = 85 if aid in laundering_accounts else 30
        blocked = "true" if aid in laundering_accounts and risk > 80 else "false"
        # ~30% of accounts have no openedDate (NULL), mirroring the Groovy loader
        has_opened_date = (int(hashlib.md5(acct_str.encode()).hexdigest(), 16) * 11) % 100 >= 30
        opened_date_val = "'2020-01-01'" if has_opened_date else "NULL"
        all_sql.append(f"MERGE INTO aml_accounts KEY(id) VALUES ({aid},'{acct_str}','{bank_str}','CHECKING',{risk},'{blocked}',{opened_date_val});")
        all_sql.append(f"MERGE INTO aml_account_bank KEY(id) VALUES ({aid},{aid},{bid},'2020-01-01','true');")

    # ── Transfers ─────────────────────────────────────────────────────────
    for t in transfer_rows:
        all_sql.append(f"MERGE INTO aml_transfers KEY(id) VALUES ({t[0]},{t[1]},{t[2]},'{t[3]}',{t[4]},'{t[5]}','{t[6]}','{t[7]}','{t[8]}');")

    # ── Alerts ────────────────────────────────────────────────────────────
    alert_count = 0
    for i, aid in enumerate(sorted(laundering_accounts), 1):
        alert_seq[0] += 1
        alid = alert_seq[0]
        all_sql.append(f"MERGE INTO aml_alerts KEY(id) VALUES ({alid},'ALERT-{alid}','SUSPICIOUS_TRANSFER','HIGH','OPEN','2022-09-01');")
        all_sql.append(f"MERGE INTO aml_account_alert KEY(id) VALUES ({alid},{aid},{alid},'2022-09-01','Laundering flag');")
        alert_count += 1

    # ── SENT_VIA ──────────────────────────────────────────────────────────
    for t in transfer_channel_rows:
        all_sql.append(f"MERGE INTO aml_transfer_channel KEY(id) VALUES ({t[0]},{t[1]},{t[2]},'{t[3]}','{t[4]}');")

    # ── Execute all SQL in ONE RunScript call ─────────────────────────────
    full_script = "\n".join(all_sql)
    print(f"Executing {len(all_sql):,} SQL statements in one pass …")
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False,
                                     encoding="utf-8") as tf:
        tf.write(full_script)
        script_file = tf.name
    try:
        _run_script_file(script_file, db_url, h2_jar)
    finally:
        os.unlink(script_file)

    stats = {
        "rowsLoaded":       rows_loaded,
        "accountsCreated":  len(account_map),
        "banksCreated":     len(bank_map),
        "transfersCreated": len(transfer_rows),
        "alertsCreated":    alert_count,
    }
    print(f"\n✓ AML H2 seed complete: {stats}")
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed AML demo data into H2")
    parser.add_argument("--csv", default=None, help="Path to aml-demo.csv")
    parser.add_argument("--max-rows", type=int, default=100_000)
    parser.add_argument("--wipe", action="store_true")
    parser.add_argument(
        "--mode",
        choices=["csvread", "sql"],
        default="csvread",
        help=(
            "'csvread' (default) — fast bulk load: write per-table CSVs and load "
            "via H2 CSVREAD() in one statement per table. "
            "'sql' — legacy row-by-row MERGE INTO statements."
        ),
    )
    args = parser.parse_args()

    repo = _repo_root()
    csv_path = args.csv or str(repo / "data" / "aml-demo.csv")
    seed(csv_path, max_rows=args.max_rows, wipe=args.wipe, mode=args.mode)

