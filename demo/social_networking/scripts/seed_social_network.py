#!/usr/bin/env python3
"""
seed_social_network.py
Seeds the Social Network demo graph into H2 or Iceberg/Trino tables.

Usage:
    # Seed H2 (default) — skips if data already present
    python3 seed_social_network.py

    # Force re-seed H2 (wipe and reload)
    python3 seed_social_network.py --wipe

    # Seed Iceberg tables — skips if data already present
    python3 seed_social_network.py --backend iceberg

    # Force re-seed Iceberg (wipe and reload)
    python3 seed_social_network.py --backend iceberg --wipe

    # Seed BOTH H2 and Iceberg for hybrid cross-datasource demo
    python3 seed_social_network.py --backend hybrid

    # Force re-seed hybrid (wipe both)
    python3 seed_social_network.py --backend hybrid --wipe

    # Seed Iceberg AND restart engine with Trino provider
    python3 seed_social_network.py --backend iceberg --restart-engine
"""

import argparse
import os
import subprocess
import sys
import time

BASE_URL = "http://localhost:7000"
TRINO_CONTAINER = "iceberg-trino"
TRINO_SERVER = "http://localhost:8080"

# BACKENDS JSON for iceberg-only mode — Trino is the default (first) backend.
# The engine uses the BackendRegistry and routes all queries to Trino.
ICEBERG_BACKENDS_JSON = (
    '[{"id":"iceberg","url":"jdbc:trino://localhost:8080/iceberg","user":"admin","driverClass":"io.trino.jdbc.TrinoDriver"}]'
)

# Maven env for iceberg mode — use BACKENDS so the engine starts in multi-backend mode
# with iceberg as the default, enabling datasource-aware routing from the mapping.
ICEBERG_ENGINE_ENV = {
    "GRAPH_PROVIDER": "sql",
    "QUERY_TRANSLATOR_BACKEND": "iceberg",
    "BACKENDS": ICEBERG_BACKENDS_JSON,
}

# BACKENDS JSON for multi-backend hybrid mode (Person/edges in H2, Company/City/Skill in Iceberg)
HYBRID_BACKENDS_JSON = (
    '[{"id":"h2","url":"jdbc:h2:file:./data/graph;AUTO_SERVER=TRUE"},'
    '{"id":"iceberg","url":"jdbc:trino://localhost:8080/iceberg","user":"admin","driverClass":"io.trino.jdbc.TrinoDriver"}]'
)


# ── Engine provider check / restart ────────────────────────────────────────────

def _get_current_provider() -> str | None:
    """Return the engine's active provider name, or None if the engine is not reachable."""
    try:
        import urllib.request
        with urllib.request.urlopen(f"{BASE_URL}/gremlin/provider", timeout=5) as resp:
            import json
            data = json.loads(resp.read())
            # Response shape: {"provider": "tinkergraph"} or {"graphProvider": "sql"} etc.
            return (data.get("provider") or data.get("graphProvider") or "").lower()
    except Exception:
        return None


def _engine_is_up() -> bool:
    try:
        import urllib.request
        with urllib.request.urlopen(f"{BASE_URL}/health", timeout=5) as resp:
            return resp.status == 200
    except Exception:
        return False


def _kill_engine() -> None:
    """Kill any process currently listening on the engine port."""
    result = subprocess.run(
        ["lsof", "-ti", f"tcp:{BASE_URL.split(':')[-1]}"],
        capture_output=True, text=True,
    )
    pids = result.stdout.strip().split()
    for pid in pids:
        if pid:
            print(f"  Stopping existing engine process (PID {pid})…")
            subprocess.run(["kill", "-9", pid], check=False)
    if pids:
        time.sleep(2)  # brief grace period for the port to free


def _start_engine_iceberg(repo_root: str) -> None:
    """Launch 'mvn exec:java' with the Iceberg/SQL provider env vars."""
    env = os.environ.copy()
    env.update(ICEBERG_ENGINE_ENV)
    print("  Starting engine with Iceberg/SQL provider…")
    print("  " + "  ".join(f"{k}={v}" for k, v in ICEBERG_ENGINE_ENV.items()))
    subprocess.Popen(
        ["mvn", "exec:java"],
        cwd=repo_root,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _get_registered_backend_ids() -> list[str]:
    """Return list of registered backend ids from the running engine, or [] on error."""
    try:
        import urllib.request, json
        with urllib.request.urlopen(f"{BASE_URL}/backends", timeout=5) as resp:
            data = json.loads(resp.read())
            return [b["id"] for b in data.get("backends", [])]
    except Exception:
        return []


def ensure_iceberg_provider(repo_root: str) -> None:
    """
    Guarantee the engine is running with the SQL provider and the 'iceberg' backend
    registered as the default (first) backend so datasource routing works correctly.

    - If the engine is already up with 'iceberg' as the first registered backend → no-op.
    - If the engine is up but iceberg is not registered → kill and restart with BACKENDS env.
    - If the engine is not up → start it with BACKENDS env.
    """
    current = _get_current_provider()

    if current == "sql":
        backend_ids = _get_registered_backend_ids()
        # iceberg must be the FIRST (default) backend for routing to work
        if backend_ids and backend_ids[0] == "iceberg":
            print("Engine is already running with the SQL provider and 'iceberg' as default backend – no restart needed.")
            return
        print(f"Engine is running with SQL provider but backends are {backend_ids} — need 'iceberg' as default.")
        print("Restarting engine with iceberg BACKENDS config…")
        _kill_engine()
    elif current is not None:
        print(f"Provider mismatch: current provider is '{current}', need 'sql'.")
        print("Restarting engine with Iceberg/SQL provider…")
        _kill_engine()
    else:
        print("Engine is not running. Starting with Iceberg/SQL provider…")

    _start_engine_iceberg(repo_root)

    print("Waiting for engine to become ready", end="", flush=True)
    for _ in range(60):
        time.sleep(3)
        print(".", end="", flush=True)
        if _engine_is_up():
            provider = _get_current_provider()
            if provider == "sql":
                print("\nEngine is ready with SQL provider.")
                return
    print("\nWarning: engine did not come up within 3 minutes. Continuing anyway…")

# ── Data-exists checks ─────────────────────────────────────────────────────────

def _h2_person_count(db_url: str, h2_jar: str) -> int:
    """Return the number of rows in sn_persons, or -1 if the table does not exist."""
    result = subprocess.run(
        ["java", "-cp", h2_jar, "org.h2.tools.Shell",
         "-url", db_url, "-user", "sa", "-password", "",
         "-sql", "SELECT COUNT(*) FROM sn_persons"],
        capture_output=True, text=True, timeout=15
    )
    if result.returncode != 0:
        return -1          # table likely doesn't exist yet
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.isdigit():
            return int(line)
    return -1


def _trino_person_count() -> int:
    """Return the number of rows in iceberg.social_network.persons, or -1 on error."""
    cmd = ["docker", "exec", "-i", TRINO_CONTAINER, "trino",
           "--server", TRINO_SERVER,
           "--execute", "SELECT COUNT(*) FROM iceberg.social_network.persons"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    if result.returncode != 0:
        return -1
    for line in result.stdout.splitlines():
        line = line.strip().strip('"')
        if line.isdigit():
            return int(line)
    return -1




PERSONS = [
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

COMPANIES = [
    (1, "TechCorp",    "Technology",    "USA", 2008, 5000),
    (2, "FinanceHub",  "Finance",       "UK",  2001, 2500),
    (3, "DataStream",  "Data Analytics","DE",  2015, 800),
    (4, "CloudNine",   "Cloud Services","USA", 2012, 3200),
    (5, "AIWorks",     "AI/ML",         "SG",  2018, 450),
]

CITIES = [
    (1, "San Francisco", "USA", 883305,  "America/Los_Angeles"),
    (2, "New York",      "USA", 8336817, "America/New_York"),
    (3, "London",        "UK",  8982000, "Europe/London"),
    (4, "Berlin",        "DE",  3769495, "Europe/Berlin"),
    (5, "Paris",         "FR",  2161000, "Europe/Paris"),
    (6, "Tokyo",         "JP",  13960000,"Asia/Tokyo"),
    (7, "Sydney",        "AU",  5312000, "Australia/Sydney"),
    (8, "Toronto",       "CA",  2930000, "America/Toronto"),
    (9, "Singapore",     "SG",  5850000, "Asia/Singapore"),
    (10,"Amsterdam",     "NL",  921402,  "Europe/Amsterdam"),
]

SKILLS = [
    (1, "Python",           "Programming"),
    (2, "Java",             "Programming"),
    (3, "SQL",              "Data"),
    (4, "Machine Learning", "AI/ML"),
    (5, "Kubernetes",       "DevOps"),
    (6, "React",            "Frontend"),
    (7, "Graph Databases",  "Data"),
    (8, "Spark",            "Data Engineering"),
]

# Edges: (id, person_a_id, person_b_id, since, strength)
KNOWS = [
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

# Edges: (id, person_id, company_id, role, start_year)
WORKS_AT = [
    (1, 1, 1, "Software Engineer",    2020),
    (2, 2, 2, "Data Analyst",         2018),
    (3, 3, 3, "ML Engineer",          2021),
    (4, 4, 1, "Engineering Manager",  2017),
    (5, 5, 4, "Backend Developer",    2022),
    (6, 6, 5, "AI Researcher",        2019),
    (7, 7, 2, "Financial Analyst",    2020),
    (8, 8, 3, "Data Scientist",       2016),
    (9, 9, 4, "Cloud Architect",      2018),
    (10,10, 5,"Junior Developer",     2022),
]

# Edges: (id, person_id, city_id, since)
LIVES_IN = [
    (1, 1, 1, "2018-01-01"),
    (2, 2, 2, "2016-01-01"),
    (3, 3, 3, "2020-01-01"),
    (4, 4, 4, "2015-01-01"),
    (5, 5, 5, "2021-01-01"),
    (6, 6, 6, "2014-01-01"),
    (7, 7, 7, "2019-01-01"),
    (8, 8, 8, "2013-01-01"),
    (9, 9, 9, "2017-01-01"),
    (10,10,10,"2022-01-01"),
]

# Edges: (id, person_id, skill_id, level, years_of_exp)
HAS_SKILL = [
    (1, 1, 1, "Expert",      5),
    (2, 1, 4, "Intermediate",2),
    (3, 2, 3, "Expert",      8),
    (4, 2, 7, "Intermediate",3),
    (5, 3, 1, "Intermediate",3),
    (6, 3, 4, "Expert",      4),
    (7, 4, 2, "Expert",      10),
    (8, 4, 5, "Expert",      7),
    (9, 5, 6, "Intermediate",2),
    (10,6, 4, "Expert",      6),
    (11,7, 3, "Intermediate",5),
    (12,8, 8, "Expert",      8),
    (13,9, 5, "Expert",      6),
    (14,10,1, "Beginner",    1),
    (15,10,6, "Beginner",    1),
]

# Edges: (id, company_id, city_id)
COMPANY_CITY = [
    (1, 1, 1),  # TechCorp → San Francisco
    (2, 2, 3),  # FinanceHub → London
    (3, 3, 4),  # DataStream → Berlin
    (4, 4, 2),  # CloudNine → New York
    (5, 5, 9),  # AIWorks → Singapore
]


# ── H2 seeding via JDBC (jaydebeapi or built-in h2 jar) ────────────────────────

def seed_h2(wipe: bool, mode: str = "csvread") -> None:
    """
    Seeds the Social Network data directly into H2.

    mode='csvread' (default) — writes per-table CSVs to a temp dir and loads
                               each table with a single H2 CSVREAD() INSERT.
    mode='sql'               — legacy: builds all MERGE INTO statements in memory
                               and runs them via H2 Shell.
    """
    import pathlib, glob, subprocess, requests, csv, tempfile, os

    # ── Upload H2 mapping ──────────────────────────────────────────────────────
    mapping_path = pathlib.Path(__file__).parent.parent / "mappings" / "social-network-h2-mapping.json"
    with open(mapping_path, "rb") as f:
        r = requests.post(f"{BASE_URL}/mapping/upload",
                          files={"file": (mapping_path.name, f, "application/json")}, timeout=15)
    r.raise_for_status()
    print(f"Mapping uploaded: {r.json().get('mappingId')}")

    # ── Locate the H2 jar in the Maven local repository ───────────────────────
    home = pathlib.Path.home()
    h2_jars = sorted(glob.glob(str(home / ".m2/repository/com/h2database/h2/**/*.jar"),
                               recursive=True))
    if not h2_jars:
        raise RuntimeError(
            "H2 jar not found in ~/.m2. Run 'mvn test-compile' once to download it.")
    h2_jar = h2_jars[-1]

    # ── Locate repo root for DB file ──────────────────────────────────────────
    repo_root = pathlib.Path(__file__).resolve().parents[3]
    db_file   = repo_root / "data" / "graph"
    db_url    = f"jdbc:h2:file:{db_file};AUTO_SERVER=TRUE"

    # ── Skip if data already present (unless --wipe) ───────────────────────────
    if not wipe:
        count = _h2_person_count(db_url, h2_jar)
        if count > 0:
            print(f"H2 already contains {count} person(s) — skipping seed. Use --wipe to reload.")
            return
        if count == 0:
            print("H2 sn_persons table exists but is empty — proceeding with seed.")

    # ── DDL ────────────────────────────────────────────────────────────────────
    ddl = """
CREATE TABLE IF NOT EXISTS sn_persons (
    id BIGINT PRIMARY KEY, name VARCHAR, age INTEGER, email VARCHAR,
    city VARCHAR, country VARCHAR, joined_at VARCHAR);
CREATE TABLE IF NOT EXISTS sn_companies (
    id BIGINT PRIMARY KEY, name VARCHAR, industry VARCHAR, country VARCHAR,
    founded INTEGER, employees INTEGER);
CREATE TABLE IF NOT EXISTS sn_cities (
    id BIGINT PRIMARY KEY, name VARCHAR, country VARCHAR, population BIGINT, timezone VARCHAR);
CREATE TABLE IF NOT EXISTS sn_skills (
    id BIGINT PRIMARY KEY, name VARCHAR, category VARCHAR);
CREATE TABLE IF NOT EXISTS sn_knows (
    id BIGINT PRIMARY KEY, person_a_id BIGINT, person_b_id BIGINT, since VARCHAR, strength VARCHAR);
CREATE TABLE IF NOT EXISTS sn_works_at (
    id BIGINT PRIMARY KEY, person_id BIGINT, company_id BIGINT, role VARCHAR, start_year INTEGER);
CREATE TABLE IF NOT EXISTS sn_lives_in (
    id BIGINT PRIMARY KEY, person_id BIGINT, city_id BIGINT, since VARCHAR);
CREATE TABLE IF NOT EXISTS sn_has_skill (
    id BIGINT PRIMARY KEY, person_id BIGINT, skill_id BIGINT, level VARCHAR, years_of_exp INTEGER);
CREATE TABLE IF NOT EXISTS sn_company_city (
    id BIGINT PRIMARY KEY, company_id BIGINT, city_id BIGINT);
"""
    wipe_sql = """
DELETE FROM sn_knows; DELETE FROM sn_works_at; DELETE FROM sn_lives_in;
DELETE FROM sn_has_skill; DELETE FROM sn_company_city;
DELETE FROM sn_persons; DELETE FROM sn_companies; DELETE FROM sn_cities; DELETE FROM sn_skills;
""" if wipe else ""

    def _run_sql(sql: str) -> None:
        result = subprocess.run(
            ["java", "-cp", h2_jar, "org.h2.tools.RunScript",
             "-url", db_url, "-user", "sa", "-password", "",
             "-script", "/dev/stdin", "-continueOnError"],
            input=sql, capture_output=True, text=True, timeout=300,
        )
        # RunScript only: if stdin doesn't work on this platform fall back to tempfile
        if result.returncode != 0:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
                tf.write(sql)
                tmp = tf.name
            try:
                r2 = subprocess.run(
                    ["java", "-cp", h2_jar, "org.h2.tools.RunScript",
                     "-url", db_url, "-user", "sa", "-password", "",
                     "-script", tmp, "-continueOnError"],
                    capture_output=True, text=True, timeout=300,
                )
                if r2.returncode != 0:
                    raise RuntimeError(f"H2 RunScript failed:\n{r2.stderr[-600:]}")
            finally:
                os.unlink(tmp)

    # Create tables (and optionally wipe)
    _run_sql(ddl + wipe_sql)

    if mode == "csvread":
        # ── Fast path: write CSV files → load with CSVREAD() ──────────────────
        def _write_csv(path: pathlib.Path, fieldnames: list, rows: list) -> None:
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                w.writerow(fieldnames)
                w.writerows(rows)

        def _load_csv(table: str, csv_file: pathlib.Path, columns: list) -> None:
            numeric_ids = {"id", "person_a_id", "person_b_id", "person_id",
                           "company_id", "city_id", "skill_id"}
            cast_cols = ", ".join(
                f"CAST({c} AS BIGINT) AS {c}" if c in numeric_ids
                else f"CAST({c} AS INTEGER) AS {c}" if c in ("age","founded","employees","start_year","years_of_exp","population")
                else f"CAST({c} AS BIGINT) AS {c}" if c == "population"
                else c
                for c in columns
            )
            sql = (
                f"INSERT INTO {table} ({', '.join(columns)}) "
                f"SELECT {cast_cols} FROM CSVREAD('{csv_file}');"
            )
            _run_sql(sql)

        with tempfile.TemporaryDirectory(prefix="sn_h2_seed_") as tmpdir:
            tmp = pathlib.Path(tmpdir)
            print(f"Writing intermediate CSVs to {tmpdir} and loading via CSVREAD() …")

            spec = [
                ("sn_persons",     ["id","name","age","email","city","country","joined_at"],          PERSONS),
                ("sn_companies",   ["id","name","industry","country","founded","employees"],           COMPANIES),
                ("sn_cities",      ["id","name","country","population","timezone"],                   CITIES),
                ("sn_skills",      ["id","name","category"],                                          SKILLS),
                ("sn_knows",       ["id","person_a_id","person_b_id","since","strength"],             KNOWS),
                ("sn_works_at",    ["id","person_id","company_id","role","start_year"],               WORKS_AT),
                ("sn_lives_in",    ["id","person_id","city_id","since"],                              LIVES_IN),
                ("sn_has_skill",   ["id","person_id","skill_id","level","years_of_exp"],              HAS_SKILL),
                ("sn_company_city",["id","company_id","city_id"],                                    COMPANY_CITY),
            ]
            for table, cols, rows in spec:
                csv_path = tmp / f"{table}.csv"
                _write_csv(csv_path, cols, rows)
                _load_csv(table, csv_path, cols)
                print(f"  ✓ {table}: {len(rows)} rows")

        print("H2 CSV bulk seed complete.")

    else:
        # ── Legacy SQL path ────────────────────────────────────────────────────
        print("Building SQL statements (legacy mode) …")
        persons_sql    = "\n".join(
            f"MERGE INTO sn_persons KEY(id) VALUES ({pid},'{name}',{age},'{email}','{city}','{country}','{joined}');"
            for pid, name, age, email, city, country, joined in PERSONS)
        companies_sql  = "\n".join(
            f"MERGE INTO sn_companies KEY(id) VALUES ({cid},'{name}','{industry}','{country}',{founded},{employees});"
            for cid, name, industry, country, founded, employees in COMPANIES)
        cities_sql     = "\n".join(
            f"MERGE INTO sn_cities KEY(id) VALUES ({cid},'{name}','{country}',{pop},'{tz}');"
            for cid, name, country, pop, tz in CITIES)
        skills_sql     = "\n".join(
            f"MERGE INTO sn_skills KEY(id) VALUES ({sid},'{name}','{category}');"
            for sid, name, category in SKILLS)
        knows_sql      = "\n".join(
            f"MERGE INTO sn_knows KEY(id) VALUES ({eid},{a},{b},'{since}','{strength}');"
            for eid, a, b, since, strength in KNOWS)
        works_at_sql   = "\n".join(
            f"MERGE INTO sn_works_at KEY(id) VALUES ({eid},{pid},{cid},'{role}',{yr});"
            for eid, pid, cid, role, yr in WORKS_AT)
        lives_in_sql   = "\n".join(
            f"MERGE INTO sn_lives_in KEY(id) VALUES ({eid},{pid},{cid},'{since}');"
            for eid, pid, cid, since in LIVES_IN)
        has_skill_sql  = "\n".join(
            f"MERGE INTO sn_has_skill KEY(id) VALUES ({eid},{pid},{sid},'{level}',{yrs});"
            for eid, pid, sid, level, yrs in HAS_SKILL)
        company_city_sql = "\n".join(
            f"MERGE INTO sn_company_city KEY(id) VALUES ({eid},{coid},{cid});"
            for eid, coid, cid in COMPANY_CITY)

        full_sql = "\n".join([persons_sql, companies_sql, cities_sql,
                              skills_sql, knows_sql, works_at_sql, lives_in_sql,
                              has_skill_sql, company_city_sql])
        print("Creating H2 tables and inserting data…")
        _run_sql(full_sql)
        print("H2 seed complete.")


# ── Iceberg/Trino seeding ──────────────────────────────────────────────────────

def trino(sql: str) -> None:
    """Execute SQL via docker exec (legacy helper, used by sql mode)."""
    cmd = ["docker", "exec", "-i", TRINO_CONTAINER, "trino",
           "--server", TRINO_SERVER, "--execute", sql]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        snippet = sql.replace("\n", " ")[:160]
        raise RuntimeError(f"Trino SQL failed: {snippet}\n{err}")


def _trino_exec(sql: str) -> list:
    """Execute SQL via trino Python client if available, else docker exec."""
    try:
        import trino as _trino  # type: ignore
        conn = _trino.dbapi.connect(
            host="localhost", port=8080, user="admin",
            http_scheme="http", request_timeout=600,
        )
        cur = conn.cursor()
        cur.execute(sql)
        try:
            return cur.fetchall()
        except Exception:
            return []
    except ImportError:
        trino(sql)
        return []


def _trino_count(sql: str) -> int:
    try:
        rows = _trino_exec(sql)
        if rows:
            return int(rows[0][0])
    except Exception:
        pass
    return -1


def _upload_csv_to_minio(csv_bytes: bytes, key: str) -> None:
    """Upload CSV bytes to the MinIO warehouse bucket."""
    bucket = "warehouse"
    try:
        import boto3  # type: ignore
        s3 = boto3.client(
            "s3",
            endpoint_url="http://localhost:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
        )
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception:
            s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key=key, Body=csv_bytes)
        print(f"    uploaded s3://{bucket}/{key} via boto3", flush=True)
    except ImportError:
        import tempfile, os
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tf:
            tf.write(csv_bytes)
            tmp_path = tf.name
        try:
            subprocess.run(
                ["mc", "alias", "set", "local", "http://localhost:9000",
                 "minioadmin", "minioadmin", "--api", "s3v4"],
                check=True, capture_output=True,
            )
            subprocess.run(
                ["mc", "cp", tmp_path, f"local/{bucket}/{key}"],
                check=True, capture_output=True,
            )
            print(f"    uploaded s3://{bucket}/{key} via mc", flush=True)
        finally:
            os.unlink(tmp_path)


def seed_iceberg(wipe: bool, mode: str = "csv") -> None:
    """
    Seed the Social Network Iceberg tables.

    mode='csv' (default) — upload per-table CSVs to MinIO, create a temporary
                           Hive external CSV table, then INSERT INTO iceberg
                           SELECT * FROM hive_csv_table.  One INSERT per table.
    mode='sql'           — original row-by-row INSERT via docker exec.
    """
    import csv as _csv, io

    print("Creating Iceberg schema…")
    _trino_exec("CREATE SCHEMA IF NOT EXISTS iceberg.social_network "
                "WITH (location = 's3://warehouse/social_network')")

    if wipe:
        print("Dropping existing tables…")
        for t in ("knows", "works_at", "lives_in", "has_skill", "company_city",
                  "persons", "companies", "cities", "skills"):
            _trino_exec(f"DROP TABLE IF EXISTS iceberg.social_network.{t}")

    # ── DDL ────────────────────────────────────────────────────────────────────
    iceberg_ddl = [
        ("persons",      "id BIGINT, name VARCHAR, age INTEGER, email VARCHAR, city VARCHAR, country VARCHAR, joined_at VARCHAR"),
        ("companies",    "id BIGINT, name VARCHAR, industry VARCHAR, country VARCHAR, founded INTEGER, employees INTEGER"),
        ("cities",       "id BIGINT, name VARCHAR, country VARCHAR, population BIGINT, timezone VARCHAR"),
        ("skills",       "id BIGINT, name VARCHAR, category VARCHAR"),
        ("knows",        "id BIGINT, person_a_id BIGINT, person_b_id BIGINT, since VARCHAR, strength VARCHAR"),
        ("works_at",     "id BIGINT, person_id BIGINT, company_id BIGINT, role VARCHAR, start_year INTEGER"),
        ("lives_in",     "id BIGINT, person_id BIGINT, city_id BIGINT, since VARCHAR"),
        ("has_skill",    "id BIGINT, person_id BIGINT, skill_id BIGINT, level VARCHAR, years_of_exp INTEGER"),
        ("company_city", "id BIGINT, company_id BIGINT, city_id BIGINT"),
    ]
    print("Creating Iceberg tables (if not exist)…")
    for t, cols in iceberg_ddl:
        _trino_exec(
            f"CREATE TABLE IF NOT EXISTS iceberg.social_network.{t} ({cols}) "
            f"WITH (format='PARQUET')"
        )

    # ── Skip-check ─────────────────────────────────────────────────────────────
    if not wipe:
        count = _trino_person_count()
        if count > 0:
            print(f"Iceberg already contains {count} person(s) — skipping. Use --wipe to reload.")
            return
        if count == 0:
            print("Iceberg persons table exists but is empty — proceeding with seed.")

    # ── Data tables ────────────────────────────────────────────────────────────
    data_spec = [
        ("persons",      ["id","name","age","email","city","country","joined_at"],         PERSONS),
        ("companies",    ["id","name","industry","country","founded","employees"],          COMPANIES),
        ("cities",       ["id","name","country","population","timezone"],                  CITIES),
        ("skills",       ["id","name","category"],                                         SKILLS),
        ("knows",        ["id","person_a_id","person_b_id","since","strength"],            KNOWS),
        ("works_at",     ["id","person_id","company_id","role","start_year"],              WORKS_AT),
        ("lives_in",     ["id","person_id","city_id","since"],                             LIVES_IN),
        ("has_skill",    ["id","person_id","skill_id","level","years_of_exp"],             HAS_SKILL),
        ("company_city", ["id","company_id","city_id"],                                   COMPANY_CITY),
    ]

    if mode == "csv":
        # ── CSV bulk load: upload to MinIO → Hive external table → INSERT SELECT ──
        _trino_exec("CREATE SCHEMA IF NOT EXISTS hive.sn_staging")

        for table, fieldnames, rows in data_spec:
            iceberg_table = f"iceberg.social_network.{table}"
            hive_tmp      = f"hive.sn_staging.{table}_csv"
            s3_prefix     = f"csv_staging/social_network/{table}"
            s3_path       = f"s3://warehouse/{s3_prefix}/"

            print(f"  [{table}] {len(rows)} rows …", end=" ", flush=True)

            # 1. Write CSV bytes
            buf = io.StringIO()
            w = _csv.writer(buf, quoting=_csv.QUOTE_MINIMAL)
            w.writerow(fieldnames)
            w.writerows(rows)
            csv_bytes = buf.getvalue().encode("utf-8")

            # 2. Upload to MinIO
            _upload_csv_to_minio(csv_bytes, f"{s3_prefix}/data.csv")

            # 3. Create Hive external CSV table
            cols_varchar = ", ".join(f"{c} VARCHAR" for c in fieldnames)
            _trino_exec(f"DROP TABLE IF EXISTS {hive_tmp}")
            _trino_exec(
                f"CREATE TABLE IF NOT EXISTS {hive_tmp} ({cols_varchar}) "
                f"WITH (format = 'CSV', skip_header_line_count = 1, "
                f"external_location = '{s3_path}')"
            )

            # 4. INSERT SELECT with type casts
            BIGINT_COLS = {"id","person_a_id","person_b_id","person_id",
                           "company_id","city_id","skill_id"}
            INT_COLS    = {"age","founded","employees","start_year","years_of_exp"}
            BIGINT_WIDE = {"population"}

            def _cast(c: str) -> str:
                if c in BIGINT_COLS or c in BIGINT_WIDE:
                    return f"TRY_CAST({c} AS BIGINT)"
                if c in INT_COLS:
                    return f"TRY_CAST({c} AS INTEGER)"
                return c

            cast_select = ", ".join(_cast(c) for c in fieldnames)
            _trino_exec(
                f"INSERT INTO {iceberg_table} ({', '.join(fieldnames)}) "
                f"SELECT {cast_select} FROM {hive_tmp}"
            )

            # 5. Drop the staging table
            _trino_exec(f"DROP TABLE IF EXISTS {hive_tmp}")

            cnt = _trino_count(f"SELECT COUNT(*) FROM {iceberg_table}")
            print(f"✓ ({cnt} rows)")

        print("Iceberg CSV bulk seed complete.")

    else:
        # ── Legacy: row-by-row INSERT via docker exec ──────────────────────────
        print("Inserting Persons…")
        for pid, name, age, email, city, country, joined in PERSONS:
            trino(f"INSERT INTO iceberg.social_network.persons VALUES "
                  f"({pid},'{name}',{age},'{email}','{city}','{country}','{joined}')")
        print("Inserting Companies…")
        for cid, name, industry, country, founded, employees in COMPANIES:
            trino(f"INSERT INTO iceberg.social_network.companies VALUES "
                  f"({cid},'{name}','{industry}','{country}',{founded},{employees})")
        print("Inserting Cities…")
        for cid, name, country, pop, tz in CITIES:
            trino(f"INSERT INTO iceberg.social_network.cities VALUES "
                  f"({cid},'{name}','{country}',{pop},'{tz}')")
        print("Inserting Skills…")
        for sid, name, category in SKILLS:
            trino(f"INSERT INTO iceberg.social_network.skills VALUES "
                  f"({sid},'{name}','{category}')")
        print("Inserting KNOWS edges…")
        for eid, a, b, since, strength in KNOWS:
            trino(f"INSERT INTO iceberg.social_network.knows VALUES "
                  f"({eid},{a},{b},'{since}','{strength}')")
        print("Inserting WORKS_AT edges…")
        for eid, pid, cid, role, yr in WORKS_AT:
            trino(f"INSERT INTO iceberg.social_network.works_at VALUES "
                  f"({eid},{pid},{cid},'{role}',{yr})")
        print("Inserting LIVES_IN edges…")
        for eid, pid, cid, since in LIVES_IN:
            trino(f"INSERT INTO iceberg.social_network.lives_in VALUES "
                  f"({eid},{pid},{cid},'{since}')")
        print("Inserting HAS_SKILL edges…")
        for eid, pid, sid, level, yrs in HAS_SKILL:
            trino(f"INSERT INTO iceberg.social_network.has_skill VALUES "
                  f"({eid},{pid},{sid},'{level}',{yrs})")
        print("Inserting LOCATED_IN edges…")
        for eid, coid, cid in COMPANY_CITY:
            trino(f"INSERT INTO iceberg.social_network.company_city VALUES "
                  f"({eid},{coid},{cid})")
        print("Iceberg seed complete.")


# ── Hybrid seeding (H2 + Iceberg) ─────────────────────────────────────────────

def seed_hybrid(wipe: bool, h2_mode: str = "csvread", iceberg_mode: str = "csv") -> None:
    """
    Seeds BOTH H2 and Iceberg and uploads the hybrid mapping.

    - H2  receives: Person vertices + KNOWS, WORKS_AT, LIVES_IN, HAS_SKILL edges
    - Iceberg receives: Company, City, Skill vertices + LOCATED_IN edge

    This is the data layout assumed by social-network-hybrid-mapping.json.
    The engine must be started with BACKENDS containing both 'h2' and 'iceberg'.
    """
    import pathlib, requests as _req

    print("=== Hybrid seed: seeding H2 (persons + relationship edges)… ===")
    seed_h2(wipe, mode=h2_mode)

    print("\n=== Hybrid seed: seeding Iceberg (companies, cities, skills + LOCATED_IN)… ===")
    seed_iceberg(wipe, mode=iceberg_mode)

    print("\n=== Hybrid seed: uploading hybrid mapping… ===")
    mapping_path = pathlib.Path(__file__).parent.parent / "mappings" / "social-network-hybrid-mapping.json"
    with open(mapping_path, "rb") as f:
        r = _req.post(
            f"{BASE_URL}/mapping/upload",
            files={"file": (mapping_path.name, f, "application/json")},
            params={"id": "social-hybrid", "name": "Social Network Hybrid", "activate": "true"},
            timeout=15,
        )
    if r.ok:
        print(f"Hybrid mapping uploaded and activated: {r.json().get('mappingId')}")
    else:
        print(f"WARNING: Hybrid mapping upload failed ({r.status_code}): {r.text[:400]}")

    print("\n=== Hybrid seed complete. ===")
    print("Engine must be started with:")
    print(f"  BACKENDS='{HYBRID_BACKENDS_JSON}' mvn exec:java")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pathlib

    parser = argparse.ArgumentParser(description="Seed Social Network demo data")
    parser.add_argument(
        "--backend",
        choices=["h2", "iceberg", "hybrid"],
        default="h2",
        help=(
            "'h2' (default) — seed H2 only; "
            "'iceberg' — seed Iceberg/Trino only; "
            "'hybrid' — seed BOTH H2 and Iceberg for cross-datasource demo"
        ),
    )
    parser.add_argument("--wipe", action="store_true", help="Drop/delete existing data before seeding")
    parser.add_argument(
        "--h2-mode",
        choices=["csvread", "sql"],
        default="csvread",
        help=(
            "H2 load mode: "
            "'csvread' (default) — write per-table CSVs and load via H2 CSVREAD() — one INSERT per table; "
            "'sql' — legacy row-by-row MERGE INTO statements"
        ),
    )
    parser.add_argument(
        "--iceberg-mode",
        choices=["csv", "sql"],
        default="csv",
        help=(
            "Iceberg load mode: "
            "'csv' (default) — upload CSVs to MinIO and load via Hive external table INSERT SELECT; "
            "'sql' — legacy row-by-row INSERT via docker exec"
        ),
    )
    parser.add_argument(
        "--restart-engine",
        action="store_true",
        help=(
            "When --backend iceberg is set, automatically restart the engine "
            "with the Iceberg/SQL provider if it is not already running with it."
        ),
    )
    parser.add_argument(
        "--repo-root",
        default=str(pathlib.Path(__file__).resolve().parents[3]),
        help="Path to the graph-query-engine repository root (used when restarting the engine).",
    )
    args = parser.parse_args()

    if args.backend == "iceberg":
        seed_iceberg(args.wipe, mode=args.iceberg_mode)
    elif args.backend == "hybrid":
        seed_hybrid(args.wipe, h2_mode=args.h2_mode, iceberg_mode=args.iceberg_mode)
    else:
        seed_h2(args.wipe, mode=args.h2_mode)

