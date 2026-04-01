#!/usr/bin/env python3
"""Generate aml_demo_queries.ipynb with SQL translation display support."""

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


def build_notebook() -> dict:
    cells = []

    cells.append(
        md(
            """
# IBM AML Demo Queries

This notebook runs Gremlin queries and shows query results.
Start backend first in another terminal:
`mvn exec:java`
"""
        )
    )

    cells.append(
        code(
            """
import requests
import pandas as pd
from typing import Dict, Any
from IPython.display import display, Markdown
from pathlib import Path

BASE_URL = "http://localhost:7000"
CSV_PATH = str(Path.cwd() / "demo/data/aml-demo.csv")
MAX_ROWS = 100000

print("Config loaded")
print("BASE_URL:", BASE_URL)
print("CSV_PATH:", CSV_PATH)
print("MAX_ROWS:", MAX_ROWS)
"""
        )
    )

    cells.append(
        code(
            """
def get_sql_explain(gremlin_query: str) -> Dict[str, Any]:
    \"\"\"Call /query/explain to get the SQL translation for a Gremlin query.\"\"\"
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


def run_gremlin_query(gremlin_query: str, tx_mode: bool = False) -> Dict[str, Any]:
    \"\"\"Execute a Gremlin query and also fetch its SQL translation.\"\"\"
    endpoint = "/gremlin/query/tx" if tx_mode else "/gremlin/query"
    result: Dict[str, Any] = {}
    try:
        response = requests.post(
            f"{BASE_URL}{endpoint}",
            json={"gremlin": gremlin_query},
            headers={"Content-Type": "application/json"},
            timeout=60,
        )
        result = response.json()
    except Exception as e:
        result = {"error": str(e)}

    # Attach SQL explanation (best-effort — may be unsupported for some traversals)
    explain = get_sql_explain(gremlin_query)
    result["_sql_explain"] = explain
    return result


def display_query_result(
    gremlin: str,
    result: Dict[str, Any],
    title: str = "",
    limit: int = 10,
    tx_mode: bool = False,
):
    if title:
        display(Markdown(f"### {title}"))

    # Gremlin traversal
    display(Markdown(f"**Gremlin:**"))
    display(Markdown(f"```groovy\\n{gremlin}\\n```"))

    # SQL translation (if available)
    explain = result.get("_sql_explain", {})
    if "error" in explain:
        display(Markdown(f"**SQL Translation:** *not available ({explain['error']})*"))
    else:
        sql = explain.get("translatedSql", "")
        params = explain.get("parameters", [])
        if sql:
            display(Markdown(f"**SQL Translation:**"))
            display(Markdown(f"```sql\\n{sql}\\n```"))
            if params:
                display(Markdown(f"**Parameters:** `{params}`"))
        else:
            display(Markdown("**SQL Translation:** *not available for this traversal*"))

    # Execution info
    gremlin_endpoint = "/gremlin/query/tx" if tx_mode else "/gremlin/query"
    display(Markdown(f"**Executed via:** `{gremlin_endpoint}`"))

    if "error" in result:
        display(Markdown(f"**Error:** {result['error']}"))
        return

    count = result.get('resultCount', 0)
    display(Markdown(f"**Result Count:** {count}"))
    rows = result.get("results", [])
    if not rows:
        return
    if isinstance(rows[0], dict):
        display(pd.DataFrame(rows[:limit]))
    else:
        for i, row in enumerate(rows[:limit], 1):
            print(f"{i}. {row}")


print("Helper functions loaded")
"""
        )
    )

    cells.append(md("## Step 0: Health check"))
    cells.append(
        code(
            """
try:
    health = requests.get(f"{BASE_URL}/health", timeout=5).text
    provider = requests.get(f"{BASE_URL}/gremlin/provider", timeout=5).json().get("provider", "unknown")
    display(Markdown(f"Status: `{health}`"))
    display(Markdown(f"Provider: `{provider}`"))
except Exception as e:
    display(Markdown(f"Health check failed: {e}"))
    display(Markdown("Action: start backend using `mvn exec:java`"))
"""
        )
    )

    cells.append(md("## Step 1: Load AML CSV and Mapping"))
    cells.append(
        code(
            """
# --- 1a: Load CSV into TinkerGraph ---
try:
    response = requests.post(
        f"{BASE_URL}/admin/load-aml-csv",
        params={"path": CSV_PATH, "maxRows": MAX_ROWS},
        timeout=120,
    )
    data = response.json()
    if "error" in data:
        display(Markdown(f"**Load failed:** {data['error']}"))
    else:
        display(Markdown("**Graph seeded — vertex & edge summary:**"))
        display(pd.DataFrame([
            {"Type": "Account vertices",     "Count": data.get("accountsCreated",    0)},
            {"Type": "Bank vertices",         "Count": data.get("banksCreated",        0)},
            {"Type": "Country vertices",      "Count": data.get("countriesCreated",    0)},
            {"Type": "Transaction vertices",  "Count": data.get("transactionsCreated", 0)},
            {"Type": "Alert vertices",        "Count": data.get("alertsCreated",       0)},
            {"Type": "TRANSFER edges",        "Count": data.get("transfersCreated",    0)},
            {"Type": "Rows loaded",           "Count": data.get("rowsLoaded",          0)},
            {"Type": "Provider",              "Count": data.get("provider",      "?")},
        ]))
except Exception as e:
    display(Markdown(f"**Load error:** {e}"))

# --- 1b: Upload AML mapping so /query/explain can show SQL translations ---
MAPPING_PATH = str(Path.cwd() / "demo/mappings/aml-mapping.json")
try:
    with open(MAPPING_PATH, "rb") as f:
        resp = requests.post(
            f"{BASE_URL}/mapping/upload",
            params={"id": "aml", "name": "AML Mapping", "activate": "true"},
            files={"file": ("aml-mapping.json", f, "application/json")},
            timeout=15,
        )
    m = resp.json()
    if "error" in m:
        display(Markdown(f"**Mapping upload failed:** {m['error']}"))
    else:
        display(Markdown(f"**AML mapping uploaded** — id=`{m.get('mappingId')}`, active=`{m.get('active')}`"))
except Exception as e:
    display(Markdown(f"**Mapping upload error:** {e}"))
"""
        )
    )

    queries = [
        # ── Simple: individual vertex/edge counts ─────────────────────────────
        {
            "section": "Simple Queries",
            "title": "S1 Count accounts",
            "description": "Count unique Account vertices.",
            "gremlin": "g.V().hasLabel('Account').count()",
            "limit": 10, "tx": False,
        },
        {
            "section": "Simple Queries",
            "title": "S2 Count banks",
            "description": "Count Bank vertices — one per distinct bank ID in the data.",
            "gremlin": "g.V().hasLabel('Bank').count()",
            "limit": 10, "tx": False,
        },
        {
            "section": "Simple Queries",
            "title": "S3 Count countries",
            "description": "Count Country vertices (10 pre-seeded jurisdictions).",
            "gremlin": "g.V().hasLabel('Country').count()",
            "limit": 10, "tx": False,
        },
        {
            "section": "Simple Queries",
            "title": "S4 Count alerts",
            "description": "Count Alert vertices — one raised per suspicious transfer.",
            "gremlin": "g.V().hasLabel('Alert').count()",
            "limit": 10, "tx": False,
        },
        {
            "section": "Simple Queries",
            "title": "S5 Count transfers",
            "description": "Count all TRANSFER edges.",
            "gremlin": "g.E().hasLabel('TRANSFER').count()",
            "limit": 10, "tx": False,
        },
        {
            "section": "Simple Queries",
            "title": "S6 Suspicious transfer count",
            "description": "Count confirmed suspicious TRANSFER edges (isLaundering=1).",
            "gremlin": "g.E().has('isLaundering','1').count()",
            "limit": 10, "tx": False,
        },
        {
            "section": "Simple Queries",
            "title": "S7 High-risk countries",
            "description": "List Country vertices with riskLevel=HIGH.",
            "gremlin": "g.V().hasLabel('Country').has('riskLevel','HIGH').project('countryCode','countryName','region','fatfBlacklist').by('countryCode').by('countryName').by('region').by('fatfBlacklist')",
            "limit": 10, "tx": False,
        },
        {
            "section": "Simple Queries",
            "title": "S8 High-severity alerts",
            "description": "Show HIGH-severity open alerts.",
            "gremlin": "g.V().hasLabel('Alert').has('severity','HIGH').project('alertId','alertType','status','raisedAt').by('alertId').by('alertType').by('status').by('raisedAt').limit(15)",
            "limit": 15, "tx": False,
        },
        # ── Complex: multi-hop, cross-entity traversals ───────────────────────
        {
            "section": "Complex Queries",
            "title": "C1 Top sender accounts",
            "description": "Accounts ranked by outgoing transfer count — find the biggest hubs.",
            "gremlin": "g.V().hasLabel('Account').project('accountId','bankId','outDegree').by('accountId').by('bankId').by(outE('TRANSFER').count()).order().by(select('outDegree'),Order.desc).limit(15)",
            "limit": 15, "tx": False,
        },
        {
            "section": "Complex Queries",
            "title": "C2 Suspicious hubs",
            "description": "Accounts with the most suspicious outgoing transfers.",
            "gremlin": "g.V().hasLabel('Account').project('accountId','bankId','suspiciousOut','totalOut').by('accountId').by('bankId').by(outE('TRANSFER').has('isLaundering','1').count()).by(outE('TRANSFER').count()).where(select('suspiciousOut').is(gt(0))).order().by(select('suspiciousOut'),Order.desc).limit(15)",
            "limit": 15, "tx": False,
        },
        {
            "section": "Complex Queries",
            "title": "C3 Account → Bank (BELONGS_TO)",
            "description": "Show which bank each account belongs to via the BELONGS_TO edge.",
            "gremlin": "g.V().hasLabel('Account').limit(15).project('accountId','bankId','bankName').by('accountId').by('bankId').by(out('BELONGS_TO').values('bankName').fold())",
            "limit": 15, "tx": False,
        },
        {
            "section": "Complex Queries",
            "title": "C4 Bank → Country (LOCATED_IN)",
            "description": "Show which country each bank is headquartered in.",
            "gremlin": "g.V().hasLabel('Bank').limit(15).project('bankId','bankName','countryCode','countryName').by('bankId').by('bankName').by('countryCode').by(out('LOCATED_IN').values('countryName').fold())",
            "limit": 15, "tx": False,
        },
        {
            "section": "Complex Queries",
            "title": "C5 Two-hop: Account → Bank → Country",
            "description": "Traverse Account→Bank→Country in two hops to find each account's jurisdiction.",
            "gremlin": "g.V().hasLabel('Account').limit(1).repeat(out('BELONGS_TO','LOCATED_IN').simplePath()).times(2).path().by('accountId').by('bankName').by('countryName').limit(10)",
            "limit": 10, "tx": False,
        },
        {
            "section": "Complex Queries",
            "title": "C6 Accounts sending to high-risk countries (SENT_VIA)",
            "description": "Find accounts that routed money via a FATF-blacklisted country.",
            "gremlin": "g.V().hasLabel('Account').where(out('SENT_VIA').has('fatfBlacklist','true')).project('accountId','bankId','riskScore').by('accountId').by('bankId').by('riskScore').limit(20)",
            "limit": 20, "tx": False,
        },
        {
            "section": "Complex Queries",
            "title": "C7 Flagged accounts with alert detail (FLAGGED_BY)",
            "description": "Show accounts under investigation with linked Alert severity.",
            "gremlin": "g.V().hasLabel('Account').where(outE('FLAGGED_BY')).project('accountId','bankId','alertCount','highSeverity').by('accountId').by('bankId').by(outE('FLAGGED_BY').count()).by(out('FLAGGED_BY').has('severity','HIGH').count()).limit(20)",
            "limit": 20, "tx": False,
        },
        {
            "section": "Complex Queries",
            "title": "C8 Cross-bank suspicious flow",
            "description": "Suspicious transfers that cross bank boundaries.",
            "gremlin": "g.E().has('isLaundering','1').project('fromBank','fromAcct','toBank','toAcct','amount','currency').by(outV().values('bankId')).by(outV().values('accountId')).by(inV().values('bankId')).by(inV().values('accountId')).by('amount').by('currency').limit(15)",
            "limit": 15, "tx": False,
        },
        {
            "section": "Complex Queries",
            "title": "C9 Three-hop money trail",
            "description": "Classic layering: follow suspicious TRANSFER hops 3 steps deep.",
            "gremlin": "g.V().hasLabel('Account').where(outE('TRANSFER').has('isLaundering','1')).limit(1).repeat(out('TRANSFER').simplePath()).times(3).path().by('accountId').limit(10)",
            "limit": 10, "tx": False,
        },
        {
            "section": "Complex Queries",
            "title": "C10 Five-hop layering chain",
            "description": "Extended 5-hop traversal to detect complex layering networks.",
            "gremlin": "g.V().hasLabel('Account').where(outE('TRANSFER').has('isLaundering','1')).limit(1).repeat(out('TRANSFER').simplePath()).times(5).path().by('accountId').limit(10)",
            "limit": 10, "tx": False,
        },
        {
            "section": "Complex Queries",
            "title": "C11 Transactional suspicious count",
            "description": "Suspicious transfer count via the transactional endpoint.",
            "gremlin": "g.E().has('isLaundering','1').count()",
            "limit": 10, "tx": True,
        },
    ]

    cells.append(
        md(
            """
## Query Sections

Each query below has its own markdown and code cell.
Every query result includes:
1. Gremlin traversal
2. Query results
"""
        )
    )

    current_section = None
    for query in queries:
        if query["section"] != current_section:
            current_section = query["section"]
            cells.append(md(f"## {current_section}"))

        cells.append(md(f"### {query['title']}\n\n{query['description']}"))
        cells.append(
            code(
                f"""
gremlin = {query['gremlin']!r}
result = run_gremlin_query(gremlin, tx_mode={query['tx']})
display_query_result(gremlin, result, title={query['title']!r}, limit={query['limit']}, tx_mode={query['tx']})
"""
            )
        )

    cells.append(md("## Done"))

    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python (.venv) GraphQueryEngine",
                "language": "python",
                "name": "graphqueryengine-venv",
            },
            "language_info": {
                "name": "python",
                "version": "3.14",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    out = repo_root / "aml_demo_queries.ipynb"
    nb = build_notebook()
    out.write_text(json.dumps(nb, indent=2), encoding="utf-8")
    print(f"Wrote {out}")
    print(f"Cells: {len(nb['cells'])}")


if __name__ == "__main__":
    main()

