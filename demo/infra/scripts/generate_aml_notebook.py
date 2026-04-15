#!/usr/bin/env python3
"""Generate AML demo notebooks (core + Iceberg comparison)."""

import json
from pathlib import Path


def md(text: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": [line + "\n" for line in text.strip().split("\n")],
    }


def code(text: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [line + "\n" for line in text.strip().split("\n")],
    }


def notebook_metadata() -> dict:
    return {
        "kernelspec": {
            "display_name": "Python (.venv) GraphQueryEngine",
            "language": "python",
            "name": "graphqueryengine-venv",
        },
        "language_info": {
            "name": "python",
            "version": "3.14",
        },
    }


def core_queries() -> list[dict]:
    return [
        {"section": "Simple Queries", "title": "S1 Count accounts", "description": "Count unique Account vertices.", "gremlin": "g.V().hasLabel('Account').count()", "limit": 10, "tx": False},
        {"section": "Simple Queries", "title": "S2 Count banks", "description": "Count Bank vertices - one per distinct bank ID in the data.", "gremlin": "g.V().hasLabel('Bank').count()", "limit": 10, "tx": False},
        {"section": "Simple Queries", "title": "S3 Count countries", "description": "Count Country vertices (10 pre-seeded jurisdictions).", "gremlin": "g.V().hasLabel('Country').count()", "limit": 10, "tx": False},
        {"section": "Simple Queries", "title": "S4 Count alerts", "description": "Count Alert vertices - one raised per suspicious transfer.", "gremlin": "g.V().hasLabel('Alert').count()", "limit": 10, "tx": False},
        {"section": "Simple Queries", "title": "S5 Count transfers", "description": "Count all TRANSFER edges.", "gremlin": "g.E().hasLabel('TRANSFER').count()", "limit": 10, "tx": False},
        {"section": "Simple Queries", "title": "S6 Suspicious transfer count", "description": "Count confirmed suspicious TRANSFER edges (isLaundering=1).", "gremlin": "g.E().has('isLaundering','1').count()", "limit": 10, "tx": False},
        {"section": "Simple Queries", "title": "S7 High-risk countries", "description": "List Country vertices with riskLevel=HIGH.", "gremlin": "g.V().hasLabel('Country').has('riskLevel','HIGH').project('countryCode','countryName','region','fatfBlacklist').by('countryCode').by('countryName').by('region').by('fatfBlacklist')", "limit": 10, "tx": False},
        {"section": "Simple Queries", "title": "S8 High-severity alerts", "description": "Show HIGH-severity open alerts.", "gremlin": "g.V().hasLabel('Alert').has('severity','HIGH').project('alertId','alertType','status','raisedAt').by('alertId').by('alertType').by('status').by('raisedAt').limit(15)", "limit": 15, "tx": False},
        {"section": "Complex Queries", "title": "C1 Top sender accounts", "description": "Accounts ranked by outgoing transfer count - find the biggest hubs.", "gremlin": "g.V().hasLabel('Account').project('accountId','bankId','outDegree').by('accountId').by('bankId').by(outE('TRANSFER').count()).order().by(select('outDegree'),Order.desc).limit(15)", "limit": 15, "tx": False},
        {"section": "Complex Queries", "title": "C2 Suspicious hubs", "description": "Accounts with the most suspicious outgoing transfers.", "gremlin": "g.V().hasLabel('Account').project('accountId','bankId','suspiciousOut','totalOut').by('accountId').by('bankId').by(outE('TRANSFER').has('isLaundering','1').count()).by(outE('TRANSFER').count()).where(select('suspiciousOut').is(gt(0))).order().by(select('suspiciousOut'),Order.desc).limit(15)", "limit": 15, "tx": False},
        {"section": "Complex Queries", "title": "C3 Account -> Bank (BELONGS_TO)", "description": "Show which bank each account belongs to via BELONGS_TO.", "gremlin": "g.V().hasLabel('Account').limit(15).project('accountId','bankId','bankName').by('accountId').by('bankId').by(out('BELONGS_TO').values('bankName').fold())", "limit": 15, "tx": False},
        {"section": "Complex Queries", "title": "C4 Bank -> Country (LOCATED_IN)", "description": "Show which country each bank is headquartered in.", "gremlin": "g.V().hasLabel('Bank').limit(15).project('bankId','bankName','countryCode','countryName').by('bankId').by('bankName').by('countryCode').by(out('LOCATED_IN').values('countryName').fold())", "limit": 15, "tx": False},
        {"section": "Complex Queries", "title": "C5 Two-hop: Account -> Bank -> Country", "description": "Traverse Account->Bank->Country in two hops.", "gremlin": "g.V().hasLabel('Account').limit(1).repeat(out('BELONGS_TO','LOCATED_IN').simplePath()).times(2).path().by('accountId').by('bankName').by('countryName').limit(10)", "limit": 10, "tx": False},
        {"section": "Complex Queries", "title": "C6 Accounts sending to high-risk countries (SENT_VIA)", "description": "Find accounts routing money via FATF-blacklisted countries.", "gremlin": "g.V().hasLabel('Account').where(out('SENT_VIA').has('fatfBlacklist','true')).project('accountId','bankId','riskScore').by('accountId').by('bankId').by('riskScore').limit(20)", "limit": 20, "tx": False},
        {"section": "Complex Queries", "title": "C7 Flagged accounts with alert detail (FLAGGED_BY)", "description": "Show investigated accounts with linked Alert severity.", "gremlin": "g.V().hasLabel('Account').where(outE('FLAGGED_BY')).project('accountId','bankId','alertCount','highSeverity').by('accountId').by('bankId').by(outE('FLAGGED_BY').count()).by(out('FLAGGED_BY').has('severity','HIGH').count()).limit(20)", "limit": 20, "tx": False},
        {"section": "Complex Queries", "title": "C8 Cross-bank suspicious flow", "description": "Suspicious transfers that cross bank boundaries.", "gremlin": "g.E().has('isLaundering','1').project('fromBank','fromAcct','toBank','toAcct','amount','currency').by(outV().values('bankId')).by(outV().values('accountId')).by(inV().values('bankId')).by(inV().values('accountId')).by('amount').by('currency').limit(15)", "limit": 15, "tx": False},
        {"section": "Complex Queries", "title": "C9 Three-hop money trail", "description": "Follow suspicious TRANSFER hops 3 steps deep.", "gremlin": "g.V().hasLabel('Account').where(outE('TRANSFER').has('isLaundering','1')).limit(1).repeat(out('TRANSFER').simplePath()).times(3).path().by('accountId').limit(10)", "limit": 10, "tx": False},
        {"section": "Complex Queries", "title": "C10 Five-hop layering chain", "description": "Extended 5-hop traversal for layering detection.", "gremlin": "g.V().hasLabel('Account').where(outE('TRANSFER').has('isLaundering','1')).limit(1).repeat(out('TRANSFER').simplePath()).times(5).path().by('accountId').limit(10)", "limit": 10, "tx": False},
        {"section": "Complex Queries", "title": "C11 Suspicious count", "description": "Suspicious transfer count.", "gremlin": "g.E().has('isLaundering','1').count()", "limit": 10, "tx": False},
    ]


def build_iceberg_notebook() -> dict:
    cells = []
    cells.append(md("""
# IBM AML Iceberg Queries

This notebook is dedicated to Iceberg/Trino comparisons.
Run first:
`./scripts/iceberg_local_up.sh`
`./scripts/iceberg_seed_trino.sh`
"""))

    cells.append(md("""
## Optional: Run Local Iceberg Setup From Notebook

This cell can run the local setup scripts directly:
1. `bash ./scripts/iceberg_local_down.sh`
2. `bash ./scripts/iceberg_local_up.sh`

Seeding is handled automatically later based on `MAX_ROWS`.
`RUN_DOWN` defaults to `False` to avoid deleting volumes unless you opt in.
"""))

    cells.append(code("""
from pathlib import Path
import subprocess
from IPython.display import display, Markdown

REPO_ROOT = Path.home() / "SourceCode/graph-query-engine"
RUN_DOWN = False
RUN_UP = True
AUTO_RUN_SETUP = False


def run_local_iceberg_setup(run_down: bool = False, run_up: bool = True) -> bool:
    if not REPO_ROOT.exists():
        display(Markdown(f"**Error:** repo path not found: `{REPO_ROOT}`"))
        return False

    steps = []
    if run_down:
        steps.append("bash ./scripts/iceberg_local_down.sh")
    if run_up:
        steps.append("bash ./scripts/iceberg_local_up.sh")

    if not steps:
        display(Markdown("No setup steps selected. Set one of `RUN_DOWN` or `RUN_UP` to `True`."))
        return False

    display(Markdown(f"Running setup in `{REPO_ROOT}`"))
    ok = True
    for i, cmd in enumerate(steps, 1):
        display(Markdown(f"**Step {i}:** `{cmd}`"))
        proc = subprocess.run(cmd, cwd=REPO_ROOT, shell=True, text=True, capture_output=True)
        out = (proc.stdout or "").strip()
        err = (proc.stderr or "").strip()
        if proc.returncode == 0:
            display(Markdown("- Status: OK"))
        else:
            display(Markdown(f"- Status: FAILED (exit {proc.returncode})"))
            ok = False
        if out:
            print(out)
        if err:
            print(err)
        if proc.returncode != 0:
            break

    display(Markdown("**Setup completed successfully.**" if ok else "**Setup failed.**"))
    return ok


if AUTO_RUN_SETUP:
    run_local_iceberg_setup(run_down=RUN_DOWN, run_up=RUN_UP)
else:
    print("Set AUTO_RUN_SETUP=True and run this cell, or call run_local_iceberg_setup(...)")
"""))

    cells.append(code("""
import requests
import pandas as pd
import subprocess
import json as _json
from typing import Dict, Any
from IPython.display import display, Markdown
from pathlib import Path

BASE_URL = "http://localhost:7000"
ICEBERG_MAPPING_PATH = str(Path.cwd() / "mappings/iceberg-local-mapping.json")
ICEBERG_MAPPING_ID = "iceberg-local"
MAX_ROWS = 100000
TRINO_CONTAINER = "iceberg-trino"
TRINO_SERVER = "http://localhost:8080"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "aml"
SHOW_PLAN = False
FORCE_RESEED = False
"""))

    cells.append(code("""
def get_sql_explain(gremlin_query: str) -> Dict[str, Any]:
    try:
        response = requests.post(
            f"{BASE_URL}/query/explain",
            json={"gremlin": gremlin_query},
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if response.ok:
            return response.json()
        return {"error": f"HTTP {response.status_code}: {response.text}"}
    except Exception as e:
        return {"error": str(e)}


def upload_iceberg_mapping() -> bool:
    try:
        with open(ICEBERG_MAPPING_PATH, "rb") as f:
            resp = requests.post(
                f"{BASE_URL}/mapping/upload",
                params={"id": ICEBERG_MAPPING_ID, "name": "Iceberg Local", "activate": "false"},
                files={"file": ("iceberg-local-mapping.json", f, "application/json")},
                timeout=15,
            )
        m = resp.json()
        if "error" not in m:
            display(Markdown(f"Iceberg mapping ready - id=`{m.get('mappingId')}`"))
            return True
        display(Markdown(f"**Mapping upload failed:** {m['error']}"))
        return False
    except Exception as e:
        display(Markdown(f"**Mapping upload error:** {e}"))
        return False


def get_iceberg_sql(gremlin_query: str, include_plan: bool = False) -> tuple[str, list, str | None, dict | None]:
    try:
        params = {"plan": "true"} if include_plan else {}
        resp = requests.post(
            f"{BASE_URL}/query/explain",
            json={"gremlin": gremlin_query},
            headers={"Content-Type": "application/json", "X-Mapping-Id": ICEBERG_MAPPING_ID},
            params=params,
            timeout=10,
        )
        if resp.ok:
            data = resp.json()
            return data.get("translatedSql", ""), data.get("parameters", []), None, data.get("plan")
        err = None
        try:
            err = resp.json().get("error")
        except Exception:
            err = resp.text
        return "", [], f"HTTP {resp.status_code}: {err}", None
    except Exception as e:
        return "", [], str(e), None


def _sql_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value).replace("'", "''")
    return f"'{s}'"


def bind_sql_parameters(sql: str, params: list) -> str:
    out = sql
    for p in params:
        if "?" not in out:
            raise ValueError("More SQL parameters were supplied than placeholders")
        out = out.replace("?", _sql_literal(p), 1)
    if "?" in out:
        raise ValueError("SQL still contains unbound placeholders")
    return out


def run_trino(sql: str) -> pd.DataFrame:
    result = subprocess.run(
        ["docker", "exec", "-i", TRINO_CONTAINER, "trino", "--server", TRINO_SERVER, "--catalog", TRINO_CATALOG, "--schema", TRINO_SCHEMA, "--output-format", "JSON", "--execute", sql],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        stderr_lines = [ln.strip() for ln in (result.stderr or "").splitlines() if ln.strip()]
        useful = [ln for ln in stderr_lines if "org.jline.utils.Log" not in ln]
        msg = (useful[-1] if useful else (stderr_lines[-1] if stderr_lines else "Trino execution failed"))
        return pd.DataFrame([{"error": msg[:500]}])
    rows = [_json.loads(l) for l in result.stdout.strip().splitlines() if l.strip()]
    return pd.DataFrame(rows if rows else [{"result": "empty"}])


def resolve_raw_source_csv() -> str | None:
    candidates = [
        REPO_ROOT / "demo/data/HI-Small_Trans.csv",
        REPO_ROOT / "data/HI-Small_Trans.csv",
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    demo_data = REPO_ROOT / "demo/data"
    if demo_data.exists():
        matches = sorted(demo_data.glob("HI-*_Trans.csv"))
        if matches:
            return str(matches[0])
    return None


def normalized_csv_path() -> str:
    return str(REPO_ROOT / "demo/data/aml-demo.csv")


def count_csv_rows(csv_path: str, max_rows: int) -> int:
    import csv
    rows = 0
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        for _ in reader:
            rows += 1
            if rows >= max_rows:
                break
    return rows


def prepare_csv_for_max_rows(auto_download: bool = True, max_rows: int = 100000) -> str | None:
    raw_source = resolve_raw_source_csv()
    if not raw_source and auto_download:
        cmd = [
            "bash",
            "./scripts/download_aml_data.sh",
            "--variant",
            "HI-Small",
            "--rows",
            str(max_rows),
        ]
        display(Markdown(f"Downloading HI-Small dataset: `{ ' '.join(cmd) }`"))
        proc = subprocess.run(cmd, cwd=REPO_ROOT, text=True, capture_output=True)
        if proc.stdout:
            print(proc.stdout)
        if proc.stderr:
            print(proc.stderr)
        if proc.returncode == 0:
            raw_source = resolve_raw_source_csv()

    if not raw_source:
        return None

    out_csv = normalized_csv_path()
    normalize_cmd = [
        "python3",
        str(REPO_ROOT / "scripts/normalize_aml.py"),
        "--src",
        str(raw_source),
        "--dst",
        str(out_csv),
        "--rows",
        str(max_rows),
    ]
    display(Markdown(f"Preparing normalized CSV for MAX_ROWS: `{ ' '.join(normalize_cmd) }`"))
    proc = subprocess.run(normalize_cmd, cwd=REPO_ROOT, text=True, capture_output=True)
    if proc.stdout:
        print(proc.stdout)
    if proc.stderr:
        print(proc.stderr)
    if proc.returncode != 0:
        return None
    return out_csv if Path(out_csv).exists() else None


def get_iceberg_transfer_count() -> int | None:
    probe = run_trino("SELECT count(*) AS c FROM transfers")
    if "error" in probe.columns:
        return None
    if "c" not in probe.columns or len(probe.index) == 0:
        return 0
    return int(probe.iloc[0]["c"])


def run_iceberg_seed_script(csv_path: str, max_rows: int) -> bool:
    cmd = [
        "bash",
        "./scripts/iceberg_seed_trino_files.sh",
        "--csv",
        str(csv_path),
        "--rows",
        str(max_rows),
        "--format",
        "csv",
    ]
    display(Markdown(f"Seeding Iceberg tables with **{max_rows:,}** rows (file-based load via Hive + object storage)..."))
    display(Markdown("Real-time progress tracking with per-step timing:"))
    proc = subprocess.Popen(
        cmd,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        print(line, end="")
    return proc.wait() == 0


def ensure_iceberg_seed_ready(auto_download: bool = True, max_rows: int = 100000) -> None:
    csv_path = prepare_csv_for_max_rows(auto_download=auto_download, max_rows=max_rows)
    if not csv_path:
        display(Markdown("**Unable to prepare normalized CSV for Iceberg seeding.**"))
        return

    expected_rows = count_csv_rows(csv_path, max_rows)
    current_rows = get_iceberg_transfer_count()

    display(Markdown(f"CSV rows considered (up to MAX_ROWS): **{expected_rows}**"))
    display(Markdown(f"Current Iceberg transfers rows: **{current_rows if current_rows is not None else 'unavailable'}**"))

    if not FORCE_RESEED and current_rows is not None and current_rows == expected_rows and expected_rows > 0:
        display(Markdown(f"Iceberg data already loaded and up-to-date (`transfers={current_rows}`)."))
        return

    seeded = run_iceberg_seed_script(csv_path, max_rows)
    if not seeded:
        display(Markdown("**Iceberg seeding failed.** Check output above."))
        return

    refreshed_rows = get_iceberg_transfer_count()
    display(Markdown(f"Iceberg seeding complete. Transfers rows: **{refreshed_rows if refreshed_rows is not None else 'unavailable'}**"))


def run_iceberg_query(gremlin_query: str) -> Dict[str, Any]:
    sql, params, err, plan = get_iceberg_sql(gremlin_query, include_plan=SHOW_PLAN)
    return {
        "gremlin": gremlin_query,
        "icebergSql": sql,
        "parameters": params,
        "plan": plan,
        "icebergError": err,
    }


def display_iceberg_result(gremlin: str, result: Dict[str, Any], title: str = "", limit: int = 10):
    if title:
        display(Markdown(f"### {title}"))
    display(Markdown("**Gremlin:**"))
    display(Markdown(f"```groovy\\n{gremlin}\\n```"))

    sql = result.get("icebergSql", "")
    params = result.get("parameters", [])
    plan = result.get("plan")
    err = result.get("icebergError")

    if not sql:
        if err:
            display(Markdown(f"*Iceberg SQL translation not available: {err}*"))
        else:
            display(Markdown("*Iceberg SQL translation not available for this query.*"))
        return

    display(Markdown("**Iceberg SQL Translation:**"))
    display(Markdown(f"```sql\\n{sql}\\n```"))
    if params:
        display(Markdown(f"**Parameters:** `{params}`"))
    if plan:
        display(Markdown("**Query Plan:**"))
        display(Markdown(f"```json\\n{_json.dumps(plan, indent=2)}\\n```"))

    try:
        executable_sql = bind_sql_parameters(sql, params)
    except Exception as e:
        display(Markdown(f"*Failed to bind SQL parameters: {e}*"))
        return

    trino_df = run_trino(executable_sql)
    if "error" in trino_df.columns:
        display(Markdown(f"**Error:** {trino_df.iloc[0]['error']}"))
        return
    display(trino_df.head(limit))


upload_iceberg_mapping()
AUTO_DOWNLOAD_IF_MISSING = True
ensure_iceberg_seed_ready(auto_download=AUTO_DOWNLOAD_IF_MISSING, max_rows=MAX_ROWS)
"""))

    cells.append(md("""
## Quick Verify

Run this cell to verify backend health, Trino connectivity, and seeded Iceberg tables.
"""))

    cells.append(code("""
def quick_verify_iceberg() -> bool:
    checks = []

    try:
        health_resp = requests.get(f"{BASE_URL}/health", timeout=5)
        health_ok = False
        health_detail = ""

        if health_resp.ok:
            try:
                payload = health_resp.json()
                if isinstance(payload, dict):
                    health_ok = str(payload.get("status", "")).strip().upper() == "OK"
                    health_detail = _json.dumps(payload)
                else:
                    text_payload = str(payload).strip()
                    health_ok = text_payload.upper() == "OK"
                    health_detail = text_payload
            except ValueError:
                text_payload = (health_resp.text or "").strip()
                health_ok = text_payload.upper() == "OK"
                health_detail = text_payload
        else:
            health_detail = f"HTTP {health_resp.status_code}: {(health_resp.text or '').strip()}"

        checks.append(("Backend", health_ok, health_detail))
    except Exception as e:
        checks.append(("Backend", False, str(e)))

    trino_ping = run_trino("SELECT 1 AS ok")
    trino_ok = "error" not in trino_ping.columns
    trino_detail = "ok" if trino_ok else trino_ping.to_string(index=False)[:240]
    checks.append(("Trino", trino_ok, trino_detail))

    table_probe = run_trino("SELECT count(*) AS c FROM accounts")
    table_ok = "error" not in table_probe.columns
    table_count = None
    if table_ok and "c" in table_probe.columns and len(table_probe.index) > 0:
        table_count = table_probe.iloc[0]["c"]
    table_detail = f"accounts={table_count}" if table_ok else table_probe.to_string(index=False)[:240]
    checks.append(("Seed data", table_ok, table_detail))

    all_ok = all(ok for _, ok, _ in checks)
    summary = "PASS" if all_ok else "FAIL"
    display(Markdown(f"**Health summary:** `{summary}`"))

    for name, ok, detail in checks:
        status = "PASS" if ok else "FAIL"
        display(Markdown(f"- **{name}:** `{status}`"))
        if not ok and detail:
            display(Markdown(f"  - detail: `{detail}`"))

    if not all_ok:
        display(Markdown("**Action:** run `run_local_iceberg_setup(run_down=False, run_up=True)` then retry."))
    return all_ok


quick_verify_iceberg()
"""))

    cells.append(md("## Query Sections\n\nThis notebook runs all AML queries via Iceberg translation + Trino execution."))

    current = None
    for q in core_queries():
        if q["section"] != current:
            current = q["section"]
            cells.append(md(f"## {current}"))
        cells.append(md(f"### {q['title']}\n\n{q['description']}"))
        cells.append(code(f"""
gremlin = {q['gremlin']!r}
result = run_iceberg_query(gremlin)
display_iceberg_result(gremlin, result, title={q['title']!r}, limit={q['limit']})
"""))

    cells.append(md("## Done"))
    return {"cells": cells, "metadata": notebook_metadata(), "nbformat": 4, "nbformat_minor": 5}


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    showcase_out = repo_root / "notebooks" / "aml_sql_showcase.ipynb"
    showcase_out.parent.mkdir(parents=True, exist_ok=True)
    showcase_nb = build_iceberg_notebook()
    showcase_out.write_text(json.dumps(showcase_nb, indent=2), encoding="utf-8")
    print(f"Wrote {showcase_out}")
    print(f"Cells: {len(showcase_nb['cells'])}")


if __name__ == "__main__":
    main()

