"""
generate_social_network_notebook.py
------------------------------------
Generates graph_database_intro.ipynb — a beginner-friendly notebook
demonstrating graph database concepts from GeeksforGeeks using a fresh
Social Network dataset (no AML data).

Run from repo root:
    python3 scripts/generate_social_network_notebook.py
"""
import json
from pathlib import Path

OUTPUT = Path(__file__).resolve().parent.parent / "graph_database_intro.ipynb"

# ---------------------------------------------------------------------------
# Cell builder helpers
# ---------------------------------------------------------------------------

def md(cell_id: str, source: str):
    return {
        "cell_type": "markdown",
        "id": cell_id,
        "metadata": {},
        "source": source.splitlines(keepends=True),
    }


def code(cell_id: str, source: str):
    return {
        "cell_type": "code",
        "execution_count": None,
        "id": cell_id,
        "metadata": {},
        "outputs": [],
        "source": source.splitlines(keepends=True),
    }


# ---------------------------------------------------------------------------
# Seed Gremlin script (single-string, kept here for readability)
# ---------------------------------------------------------------------------
SEED_GREMLIN = """\
// ── Clear any existing data ───────────────────────────────────────────────
g.E().drop().iterate()
g.V().drop().iterate()

// ── Cities ────────────────────────────────────────────────────────────────
def nyc = g.addV('City').property('name','New York').property('country','USA').property('timezone','EST').next()
def sfo = g.addV('City').property('name','San Francisco').property('country','USA').property('timezone','PST').next()
def lon = g.addV('City').property('name','London').property('country','UK').property('timezone','GMT').next()
def ber = g.addV('City').property('name','Berlin').property('country','Germany').property('timezone','CET').next()

// ── Companies ─────────────────────────────────────────────────────────────
def techcorp  = g.addV('Company').property('name','TechCorp').property('industry','Technology').property('country','USA').next()
def startupx  = g.addV('Company').property('name','StartupX').property('industry','FinTech').property('country','USA').next()
def datasys   = g.addV('Company').property('name','DataSystems').property('industry','Analytics').property('country','UK').next()
def cloudbase = g.addV('Company').property('name','CloudBase').property('industry','Cloud').property('country','Germany').next()

// ── Skills ────────────────────────────────────────────────────────────────
def python = g.addV('Skill').property('name','Python').property('category','Programming').next()
def java   = g.addV('Skill').property('name','Java').property('category','Programming').next()
def ml     = g.addV('Skill').property('name','Machine Learning').property('category','AI').next()
def cloud  = g.addV('Skill').property('name','Cloud').property('category','Infrastructure').next()
def sql    = g.addV('Skill').property('name','SQL').property('category','Data').next()

// ── People ────────────────────────────────────────────────────────────────
def alice   = g.addV('Person').property('name','Alice').property('age',30).property('email','alice@techcorp.com').property('city','New York').next()
def bob     = g.addV('Person').property('name','Bob').property('age',28).property('email','bob@startupx.com').property('city','San Francisco').next()
def charlie = g.addV('Person').property('name','Charlie').property('age',35).property('email','charlie@techcorp.com').property('city','New York').next()
def diana   = g.addV('Person').property('name','Diana').property('age',32).property('email','diana@datasys.co.uk').property('city','London').next()
def eve     = g.addV('Person').property('name','Eve').property('age',27).property('email','eve@cloudbase.de').property('city','Berlin').next()
def frank   = g.addV('Person').property('name','Frank').property('age',40).property('email','frank@startupx.com').property('city','San Francisco').next()

// ── KNOWS relationships ───────────────────────────────────────────────────
alice.addEdge('KNOWS', bob,     'since','2020-01-15', 'strength','strong')
alice.addEdge('KNOWS', eve,     'since','2019-06-01', 'strength','medium')
bob.addEdge('KNOWS',   charlie, 'since','2021-03-10', 'strength','strong')
bob.addEdge('KNOWS',   frank,   'since','2018-11-20', 'strength','strong')
charlie.addEdge('KNOWS', diana, 'since','2022-05-05', 'strength','medium')
diana.addEdge('KNOWS',   eve,   'since','2020-09-12', 'strength','weak')
eve.addEdge('KNOWS',   frank,   'since','2021-07-30', 'strength','medium')

// ── WORKS_AT relationships ────────────────────────────────────────────────
alice.addEdge('WORKS_AT',   techcorp,  'role','Engineer',        'since','2018-01-01', 'isCurrent','true')
bob.addEdge('WORKS_AT',     startupx,  'role','Product Manager', 'since','2020-06-01', 'isCurrent','true')
charlie.addEdge('WORKS_AT', techcorp,  'role','Architect',       'since','2015-03-01', 'isCurrent','true')
diana.addEdge('WORKS_AT',   datasys,   'role','Data Scientist',  'since','2019-09-01', 'isCurrent','true')
eve.addEdge('WORKS_AT',     cloudbase, 'role','DevOps Engineer', 'since','2021-01-15', 'isCurrent','true')
frank.addEdge('WORKS_AT',   startupx,  'role','CTO',             'since','2017-05-01', 'isCurrent','true')

// ── LIVES_IN relationships ────────────────────────────────────────────────
alice.addEdge('LIVES_IN',   nyc, 'since','2015-01-01')
bob.addEdge('LIVES_IN',     sfo, 'since','2019-06-01')
charlie.addEdge('LIVES_IN', nyc, 'since','2010-03-01')
diana.addEdge('LIVES_IN',   lon, 'since','2018-09-01')
eve.addEdge('LIVES_IN',     ber, 'since','2020-01-15')
frank.addEdge('LIVES_IN',   sfo, 'since','2016-05-01')

// ── HAS_SKILL relationships ───────────────────────────────────────────────
alice.addEdge('HAS_SKILL',   python, 'level','Expert',       'yearsExp',6)
alice.addEdge('HAS_SKILL',   ml,     'level','Intermediate', 'yearsExp',3)
alice.addEdge('HAS_SKILL',   sql,    'level','Expert',       'yearsExp',7)
bob.addEdge('HAS_SKILL',     sql,    'level','Beginner',     'yearsExp',1)
charlie.addEdge('HAS_SKILL', java,   'level','Expert',       'yearsExp',12)
charlie.addEdge('HAS_SKILL', cloud,  'level','Advanced',     'yearsExp',5)
diana.addEdge('HAS_SKILL',   python, 'level','Advanced',     'yearsExp',5)
diana.addEdge('HAS_SKILL',   ml,     'level','Expert',       'yearsExp',8)
eve.addEdge('HAS_SKILL',     cloud,  'level','Expert',       'yearsExp',6)
eve.addEdge('HAS_SKILL',     python, 'level','Intermediate', 'yearsExp',3)
frank.addEdge('HAS_SKILL',   java,   'level','Expert',       'yearsExp',15)
frank.addEdge('HAS_SKILL',   cloud,  'level','Advanced',     'yearsExp',4)

// ── Company LOCATED_IN City ───────────────────────────────────────────────
techcorp.addEdge('LOCATED_IN',  nyc, 'isHQ','true')
startupx.addEdge('LOCATED_IN',  sfo, 'isHQ','true')
datasys.addEdge('LOCATED_IN',   lon, 'isHQ','true')
cloudbase.addEdge('LOCATED_IN', ber, 'isHQ','true')

return [seeded: true, persons: 6, companies: 4, cities: 4, skills: 5]
"""

# ---------------------------------------------------------------------------
# Cell definitions
# ---------------------------------------------------------------------------
cells = []

cells.append(md("nb-title", """\
# What is a Graph Database?

> **Based on:** [GeeksforGeeks — What is Graph Database?](https://www.geeksforgeeks.org/dbms/what-is-graph-database/)

A **graph database** stores data as a network of **nodes** (entities) and **edges** (relationships).
Unlike relational databases that join rows across tables, graph databases make relationships
first-class citizens, which makes traversal queries extremely natural and efficient.

## Core concepts

| Concept | Description | Example |
|---------|-------------|---------|
| **Node (Vertex)** | An entity or object in the graph | `Person`, `Company`, `City` |
| **Edge (Relationship)** | A directed connection between two nodes | `KNOWS`, `WORKS_AT`, `LIVES_IN` |
| **Property** | A key-value attribute on a node or edge | `name`, `age`, `since` |
| **Label** | A category/type tag on a node | `Person`, `Skill` |

## Our demo graph — Social Network

We will build a small social-network graph that mirrors the example on GeeksforGeeks:

```
Alice ──[KNOWS]──► Bob ──[KNOWS]──► Charlie ──[KNOWS]──► Diana
  │                 │                                        │
[KNOWS]         [WORKS_AT]                               [KNOWS]
  │                 │                                        │
 Eve            StartupX                                   Eve
  │              (FinTech)                                   │
[WORKS_AT]     [LOCATED_IN]                             [WORKS_AT]
  │                 │                                        │
CloudBase      San Francisco                            CloudBase
```

**Nodes:** Person · Company · City · Skill
**Edges:** KNOWS · WORKS_AT · LIVES_IN · HAS_SKILL · LOCATED_IN

---
**Prerequisites:**
1. Backend running: `mvn exec:java` (in repo root)
2. Run the setup cells below — they seed fresh data automatically (no CSV needed)
"""))

# ── Step 0: Imports ─────────────────────────────────────────────────────────
cells.append(md("section-imports", "---\n## Step 0 — Imports & helper functions"))

cells.append(code("cell-imports", """\
import json
import requests
import pandas as pd
from pathlib import Path
from IPython.display import display, Markdown

BASE_URL     = "http://localhost:7000"
MAPPING_PATH = Path.cwd() / "mappings/social-network-mapping.json"


def gremlin(query: str) -> dict:
    \"\"\"Execute a Gremlin query and return the full JSON response.\"\"\"
    try:
        r = requests.post(f"{BASE_URL}/gremlin/query", json={"gremlin": query}, timeout=QUERY_TIMEOUT)
        return r.json() if r.ok else {"error": f"HTTP {r.status_code}: {r.text}"}
    except Exception as e:
        return {"error": str(e)}


def show(query: str, title: str = "", limit: int = 20):
    \"\"\"Run a Gremlin query and display results. SQL translation is logged server-side only.\"\"\"
    if title:
        display(Markdown(f"### {title}"))
    display(Markdown(f"**Gremlin:**\\n```groovy\\n{query}\\n```"))
    res = gremlin(query)
    if "error" in res:
        display(Markdown(f"\\u26a0\\ufe0f **Error:** {res['error']}"))
        return
    rows = res.get("results", [])
    count = res.get("resultCount", len(rows))
    display(Markdown(f"**Result count:** {count}"))
    if rows:
        sample = rows[:limit]
        if isinstance(sample[0], dict):
            display(pd.DataFrame(sample))
        else:
            for i, v in enumerate(sample, 1):
                print(f"  {i:2d}. {v}")


# Health check
try:
    health   = requests.get(f"{BASE_URL}/health", timeout=5).text
    provider = requests.get(f"{BASE_URL}/health", timeout=5).json().get("provider", "?")
    display(Markdown(f"\\u2705 **Backend healthy** \\u2014 `{health}` | Provider: `{provider}`"))
except Exception as e:
    display(Markdown(f"\\u274c **Backend not reachable:** {e}  \\n> Start it with: `mvn exec:java`"))
"""))

# ── Step 1: Mapping upload ───────────────────────────────────────────────────
cells.append(md("section-mapping", """\
---
## Step 1 — Upload the Social Network mapping

The mapping file (`mappings/social-network-mapping.json`) tells the engine how
graph labels and properties correspond to relational SQL tables.

**Graph schema:**
```
Person  --[KNOWS]------> Person
Person  --[WORKS_AT]---> Company
Person  --[LIVES_IN]---> City
Person  --[HAS_SKILL]--> Skill
Company --[LOCATED_IN]-> City
```
"""))

cells.append(code("cell-mapping", """\
if not MAPPING_PATH.exists():
    display(Markdown(f"\\u274c Mapping file not found: `{MAPPING_PATH}`"))
else:
    with open(MAPPING_PATH, "rb") as f:
        resp = requests.post(
            f"{BASE_URL}/mapping/upload",
            files={"file": (MAPPING_PATH.name, f, "application/json")},
            timeout=15,
        )
    if resp.ok:
        display(Markdown(f"\\u2705 **Mapping uploaded** \\u2014 HTTP {resp.status_code}"))
        display(Markdown(f"```json\\n{json.dumps(resp.json(), indent=2)}\\n```"))
    else:
        display(Markdown(f"\\u274c Upload failed HTTP {resp.status_code}: {resp.text}"))
"""))

# ── Step 2: Seed data ────────────────────────────────────────────────────────
cells.append(md("section-seed", """\
---
## Step 2 — Seed the Social Network graph

We insert all nodes and relationships directly via a Gremlin mutation script.
**No CSV or external file is required.**

| Type | Nodes |
|------|-------|
| 👤 Person | Alice (30), Bob (28), Charlie (35), Diana (32), Eve (27), Frank (40) |
| 🏢 Company | TechCorp, StartupX, DataSystems, CloudBase |
| 🏙️ City | New York, San Francisco, London, Berlin |
| 🛠️ Skill | Python, Java, Machine Learning, Cloud, SQL |
"""))

# Embed the Gremlin seed script as a Python triple-quoted string in the cell
seed_cell_src = (
    'SEED_SCRIPT = r"""\n'
    + SEED_GREMLIN
    + '"""\n\n'
    'res = gremlin(SEED_SCRIPT)\n'
    'if "error" in res:\n'
    '    display(Markdown(f"\\u274c **Seed failed:** {res[\'error\']}\\n\\nCheck that the backend is running."))\n'
    'else:\n'
    '    stats = (res.get("results") or [{}])[0]\n'
    '    display(Markdown(\n'
    '        f"\\u2705 **Graph seeded successfully!**\\n"\n'
    '        f"- \\U0001f464 Persons   : {stats.get(\'persons\',\'?\')}  (Alice, Bob, Charlie, Diana, Eve, Frank)\\n"\n'
    '        f"- \\U0001f3e2 Companies : {stats.get(\'companies\',\'?\')}  (TechCorp, StartupX, DataSystems, CloudBase)\\n"\n'
    '        f"- \\U0001f3d9 Cities    : {stats.get(\'cities\',\'?\')}  (New York, San Francisco, London, Berlin)\\n"\n'
    '        f"- \\U0001f6e0 Skills    : {stats.get(\'skills\',\'?\')}  (Python, Java, Machine Learning, Cloud, SQL)"\n'
    '    ))\n'
)
cells.append(code("cell-seed", seed_cell_src))

# ── Step 3: Overview ─────────────────────────────────────────────────────────
cells.append(md("section-overview", "---\n## Step 3 — Verify graph contents (counts)\n"))

cells.append(code("cell-overview", """\
def cnt(label: str, is_edge: bool = False) -> int:
    step = "g.E()" if is_edge else "g.V()"
    r = gremlin(f"{step}.hasLabel('{label}').count()")
    return (r.get("results") or [0])[0]

rows_v = [(lbl, cnt(lbl)) for lbl in ["Person", "Company", "City", "Skill"]]
rows_e = [(lbl, cnt(lbl, True)) for lbl in ["KNOWS", "WORKS_AT", "LIVES_IN", "HAS_SKILL", "LOCATED_IN"]]

display(Markdown("**Vertices**"))
display(pd.DataFrame(rows_v, columns=["Label", "Count"]))
display(Markdown("**Edges**"))
display(pd.DataFrame(rows_e, columns=["Label", "Count"]))
"""))

# ── Q1: List all people ────────────────────────────────────────────────────
cells.append(md("q1-title", """\
---
## Query 1 — List all people

The most basic graph query: fetch all `Person` nodes and their properties.

**Concept:** `g.V().hasLabel(...)` scans all vertices of a given type —
equivalent to `SELECT * FROM persons` in SQL.
"""))

cells.append(code("q1-code", """\
show(
    "g.V().hasLabel('Person').valueMap('name','age','city').limit(10)",
    title="Q1 — All people in the social network",
)
"""))

# ── Q2: Find by property ───────────────────────────────────────────────────
cells.append(md("q2-title", """\
---
## Query 2 — Find a specific person by property

**Concept:** `has(key, value)` filters nodes by a property — like `WHERE name = 'Alice'`.
This is the fundamental **node lookup** pattern in graph databases.
"""))

cells.append(code("q2-code", """\
show(
    "g.V().hasLabel('Person').has('name','Alice').valueMap('name','age','email','city')",
    title="Q2 — Find Alice by name",
)
"""))

# ── Q3: 1-hop ──────────────────────────────────────────────────────────────
cells.append(md("q3-title", """\
---
## Query 3 — Direct friends (1-hop KNOWS)

**Concept:** `out('KNOWS')` traverses the outgoing `KNOWS` edges from a node.
This is the iconic graph database example — finding direct connections is a
**single-hop traversal** that requires no JOIN in a graph database.
"""))

cells.append(code("q3-code", """\
show(
    "g.V().hasLabel('Person').has('name','Alice').out('KNOWS').values('name')",
    title="Q3 — Who does Alice directly know?",
)
"""))

# ── Q4: 2-hop ──────────────────────────────────────────────────────────────
cells.append(md("q4-title", """\
---
## Query 4 — Friends-of-friends (2-hop traversal)

**Concept:** Chaining `out('KNOWS').out('KNOWS')` performs a **2-hop traversal**.
This is where graph databases truly shine — in SQL this requires two self-JOINs;
in Gremlin you simply chain the steps.
"""))

cells.append(code("q4-code", """\
show(
    "g.V().hasLabel('Person').has('name','Alice').out('KNOWS').out('KNOWS').values('name').dedup()",
    title="Q4 — Friends-of-friends of Alice (2-hop)",
)
"""))

# ── Q5: Works at ──────────────────────────────────────────────────────────
cells.append(md("q5-title", """\
---
## Query 5 — Where does a person work?

**Concept:** Traversing from a `Person` to a `Company` via the `WORKS_AT` edge.
This is a **cross-entity relationship traversal** — the core strength of graph databases.
"""))

cells.append(code("q5-code", """\
show(
    "g.V().hasLabel('Person').has('name','Bob').out('WORKS_AT').valueMap('name','industry','country')",
    title="Q5 — Where does Bob work?",
)
"""))

# ── Q6: Colleagues ─────────────────────────────────────────────────────────
cells.append(md("q6-title", """\
---
## Query 6 — Colleagues (people at the same company)

**Concept:** Person → Company → back to Person using `in()` (incoming edges).
This finds all people who share a relationship with the same node —
a classic **bi-directional graph pattern**.
"""))

cells.append(code("q6-code", """\
show(
    "g.V().hasLabel('Person').has('name','Alice').out('WORKS_AT').in('WORKS_AT').values('name').dedup()",
    title="Q6 — Who are Alice's colleagues at the same company?",
)
"""))

# ── Q7: 3-hop path ─────────────────────────────────────────────────────────
cells.append(md("q7-title", """\
---
## Query 7 — Multi-hop path: Person → Company → City

**Concept:** Multi-hop traversal across three different node types.
The `path()` step captures the complete traversal path end-to-end.
In SQL this would require 2 JOINs; in Gremlin it reads almost like natural language.
"""))

cells.append(code("q7-code", """\
show(
    "g.V().hasLabel('Person').out('WORKS_AT').out('LOCATED_IN').path().by('name').limit(10)",
    title="Q7 — Full path: Person -> Company -> City",
)
"""))

# ── Q8: Edge property filter ───────────────────────────────────────────────
cells.append(md("q8-title", """\
---
## Query 8 — Filter by edge property

**Concept:** `has()` applied on an **edge** (not a vertex) during traversal.
Here we filter to find only **strong** friendships.
Edge properties are first-class citizens in graph databases — no extra join table needed.
"""))

cells.append(code("q8-code", """\
show(
    "g.V().hasLabel('Person').has('name','Alice').outE('KNOWS').has('strength','strong').inV().values('name')",
    title="Q8 — Alice's strong connections only (edge property filter)",
)
"""))

# ── Q9: Reverse lookup ─────────────────────────────────────────────────────
cells.append(md("q9-title", """\
---
## Query 9 — Reverse lookup: who has a given skill?

**Concept:** Start from a `Skill` node and traverse backwards to `Person` using `in()`.
This is a **reverse lookup** — in SQL you'd query `WHERE skill = 'Python'` with a JOIN;
in a graph you simply follow the edge in reverse.
"""))

cells.append(code("q9-code", """\
show(
    "g.V().hasLabel('Skill').has('name','Python').in('HAS_SKILL').values('name')",
    title="Q9 — Who knows Python? (reverse traversal)",
)
"""))

# ── Q10: Sub-traversal predicate ────────────────────────────────────────────
cells.append(md("q10-title", """\
---
## Query 10 — Find all Expert-level skill holders

**Concept:** `where(outE(...).has(...))` — a **sub-traversal predicate**.
Filter people who have at least one `HAS_SKILL` edge with `level='Expert'`.
This is equivalent to a correlated sub-query in SQL.
"""))

cells.append(code("q10-code", """\
show(
    "g.V().hasLabel('Person').where(outE('HAS_SKILL').has('level','Expert')).values('name')",
    title="Q10 — People with at least one Expert-level skill",
)
"""))

# ── Q11: Recommendation ────────────────────────────────────────────────────
cells.append(md("q11-title", """\
---
## Query 11 — Skill recommendation

**Concept:** Graph databases are ideal for **recommendation engines**.
This query finds skills held by Alice's connections that Alice does not already have —
the classic *"people who know X also use Y"* pattern.
"""))

cells.append(code("q11-code", """\
# Alice's own skills
alice_skills = set(
    gremlin("g.V().hasLabel('Person').has('name','Alice').out('HAS_SKILL').values('name')")
    .get("results", [])
)
display(Markdown(f"**Alice's current skills:** {sorted(alice_skills)}"))

# Skills that Alice's friends have
friend_skills = set(
    gremlin("g.V().hasLabel('Person').has('name','Alice').out('KNOWS').out('HAS_SKILL').values('name').dedup()")
    .get("results", [])
)
display(Markdown(f"**Friends' combined skills:** {sorted(friend_skills)}"))

# Recommendation = friend skills Alice does not have
recommended = friend_skills - alice_skills
display(Markdown(f"\\n### Q11 Result — Recommended skills for Alice: **{sorted(recommended)}**"))
"""))

# ── Q12: Degree centrality ─────────────────────────────────────────────────
cells.append(md("q12-title", """\
---
## Query 12 — Degree centrality (most connected person)

**Concept:** Counting the number of edges per node gives **degree centrality** —
a fundamental graph metric for identifying influencers or hubs in a network.
"""))

cells.append(code("q12-code", """\
people = gremlin("g.V().hasLabel('Person').values('name')").get("results", [])

centrality = []
for name in people:
    out_deg = (gremlin(f"g.V().hasLabel('Person').has('name','{name}').out('KNOWS').count()").get("results") or [0])[0]
    in_deg  = (gremlin(f"g.V().hasLabel('Person').has('name','{name}').in('KNOWS').count()").get("results")  or [0])[0]
    centrality.append({
        "Person": name,
        "Outgoing (initiates)": out_deg,
        "Incoming (is known by)": in_deg,
        "Total Degree": out_deg + in_deg,
    })

df = pd.DataFrame(centrality).sort_values("Total Degree", ascending=False).reset_index(drop=True)
display(Markdown("### Q12 — Who is the most connected person? (Degree Centrality)"))
display(df)
"""))

# ── Q13: Shortest path ────────────────────────────────────────────────────
cells.append(md("q13-title", """\
---
## Query 13 — Shortest path between two people

**Concept:** `repeat(...).until(...)` implements **breadth-first search**
to find the shortest connection path — the classic
> *Six Degrees of Separation* problem.

Graph databases solve this natively; SQL requires complex recursive CTEs.
"""))

cells.append(code("q13-code", """\
display(Markdown("### Q13 — Shortest path: Alice \\u2192 Diana (via KNOWS)"))

q = (
    "g.V().hasLabel('Person').has('name','Alice')"
    ".repeat(out('KNOWS').simplePath())"
    ".until(has('name','Diana'))"
    ".path().by('name').limit(3)"
)
display(Markdown(f"**Gremlin:**\\n```groovy\\n{q}\\n```"))

res = gremlin(q)
if "error" in res:
    display(Markdown(f"\\u26a0\\ufe0f **Error:** {res['error']}"))
else:
    paths = res.get("results", [])
    if paths:
        for i, path in enumerate(paths, 1):
            display(Markdown(f"**Path {i}:** `{' \\u2192 '.join(path)}`  ({len(path)-1} hops)"))
    else:
        display(Markdown("No path found."))
"""))

# ── Q14: Same city ─────────────────────────────────────────────────────────
cells.append(md("q14-title", """\
---
## Query 14 — People who live and work in the same city

**Concept:** A graph **pattern matching** query where two traversal paths
must converge on the same node.
Here: home city (`LIVES_IN`) must equal company city (`WORKS_AT → LOCATED_IN`).
"""))

cells.append(code("q14-code", """\
display(Markdown("### Q14 — People whose home city matches their company's office city"))

people = gremlin("g.V().hasLabel('Person').values('name')").get("results", [])
matches = []
for name in people:
    home = set(gremlin(f"g.V().hasLabel('Person').has('name','{name}').out('LIVES_IN').values('name')").get("results", []))
    work = set(gremlin(f"g.V().hasLabel('Person').has('name','{name}').out('WORKS_AT').out('LOCATED_IN').values('name')").get("results", []))
    overlap = home & work
    if overlap:
        matches.append({"Person": name, "Shared City": sorted(overlap)[0], "Match": "\\u2705"})

if matches:
    display(pd.DataFrame(matches))
else:
    display(Markdown("No exact city matches found."))
"""))

# ── Q15: Group by industry ─────────────────────────────────────────────────
cells.append(md("q15-title", """\
---
## Query 15 — Group people by company industry

**Concept:** Aggregation across a relationship.
Equivalent to `SELECT industry, GROUP_CONCAT(name) FROM persons JOIN companies GROUP BY industry`.
In Gremlin this is a fold over traversal results.
"""))

cells.append(code("q15-code", """\
display(Markdown("### Q15 — People grouped by their company's industry"))

people = gremlin("g.V().hasLabel('Person').values('name')").get("results", [])
industry_map: dict = {}
for name in people:
    industries = gremlin(
        f"g.V().hasLabel('Person').has('name','{name}').out('WORKS_AT').values('industry')"
    ).get("results", [])
    for ind in industries:
        industry_map.setdefault(ind, []).append(name)

rows = [
    {"Industry": ind, "People": ", ".join(sorted(names)), "Count": len(names)}
    for ind, names in sorted(industry_map.items())
]
display(pd.DataFrame(rows))
"""))

# ── Summary ────────────────────────────────────────────────────────────────
cells.append(md("section-summary", """\
---
## Summary — Why Graph Databases?

| Query | Graph (Gremlin) | Relational (SQL) |
|---|---|---|
| Find a node | `g.V().has('name','Alice')` | `SELECT * FROM persons WHERE name='Alice'` |
| 1-hop relationship | `out('KNOWS')` | `JOIN knows ON ...` |
| 2-hop traversal | `out('KNOWS').out('KNOWS')` | 2 self-JOINs |
| N-hop / shortest path | `repeat(out()).until(...)` | Recursive CTE (complex) |
| Filter on edge property | `outE().has('strength','strong')` | Extra join table needed |
| Pattern match | `where(out('X').in('Y'))` | Correlated sub-query |
| Recommendation | Chain traversals naturally | Multiple self-joins |

### Key takeaways (from GeeksforGeeks)

1. **Nodes = Entities** — People, Companies, Cities, Skills
2. **Edges = Relationships** — KNOWS, WORKS_AT, LIVES_IN, HAS_SKILL
3. **Properties on both nodes and edges** — age, strength, level, since
4. **Traversal is O(local)** — follows edges, doesn't scan entire tables
5. **Natural for connected data** — social networks, recommendations, fraud, knowledge graphs

### Real-world use cases for graph databases
- 🤝 **Social Networks** — friend-of-friend discovery, influencer detection
- 🛒 **Recommendation Engines** — "people who know X also use Y"
- 🔍 **Fraud Detection** — suspicious paths in financial transaction graphs
- 🧠 **Knowledge Graphs** — semantic relationships between concepts
- 🌐 **Network / IT management** — dependency graphs, blast-radius analysis
"""))

# ---------------------------------------------------------------------------
# Assemble & write notebook
# ---------------------------------------------------------------------------
nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "name": "python",
            "version": "3.11.0",
        },
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

with open(OUTPUT, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)

# Summary
code_cells = sum(1 for c in cells if c["cell_type"] == "code")
md_cells   = sum(1 for c in cells if c["cell_type"] == "markdown")
print(f"✅  Written: {OUTPUT}")
print(f"   Total cells  : {len(cells)}  ({md_cells} markdown, {code_cells} code)")

