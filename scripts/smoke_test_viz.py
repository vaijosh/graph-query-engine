"""
Smoke-tests the matplotlib + pyvis visualization code used in
social_networking_demo_sql.ipynb (Step 4 cells).
Run with the project venv:
    .venv/bin/python3 scripts/smoke_test_viz.py
"""
import os, sys

# ── 1. Build the NetworkX graph ───────────────────────────────────────────────
import networkx as nx

G = nx.DiGraph()

PERSONS   = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"]
COMPANIES = ["TechCorp", "StartupX", "DataSystems", "CloudBase"]
CITIES    = ["New York", "San Francisco", "London", "Berlin"]
SKILLS    = ["Python", "Java", "Machine Learning", "Cloud", "SQL"]

for n in PERSONS:   G.add_node(n, ntype="Person")
for n in COMPANIES: G.add_node(n, ntype="Company")
for n in CITIES:    G.add_node(n, ntype="City")
for n in SKILLS:    G.add_node(n, ntype="Skill")

for a, b in [("Alice","Bob"),("Alice","Eve"),("Bob","Charlie"),
             ("Bob","Frank"),("Charlie","Diana"),("Diana","Eve"),("Eve","Frank")]:
    G.add_edge(a, b, etype="KNOWS")
for p, c in [("Alice","TechCorp"),("Bob","StartupX"),("Charlie","TechCorp"),
             ("Diana","DataSystems"),("Eve","CloudBase"),("Frank","StartupX")]:
    G.add_edge(p, c, etype="WORKS_AT")
for p, c in [("Alice","New York"),("Bob","San Francisco"),("Charlie","New York"),
             ("Diana","London"),("Eve","Berlin"),("Frank","San Francisco")]:
    G.add_edge(p, c, etype="LIVES_IN")
for p, s in [("Alice","Python"),("Alice","Machine Learning"),("Alice","SQL"),
             ("Bob","SQL"),("Charlie","Java"),("Charlie","Cloud"),
             ("Diana","Python"),("Diana","Machine Learning"),
             ("Eve","Cloud"),("Eve","Python"),
             ("Frank","Java"),("Frank","Cloud")]:
    G.add_edge(p, s, etype="HAS_SKILL")
for c, city in [("TechCorp","New York"),("StartupX","San Francisco"),
                ("DataSystems","London"),("CloudBase","Berlin")]:
    G.add_edge(c, city, etype="LOCATED_IN")

print(f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

# ── 2. Matplotlib static image ────────────────────────────────────────────────
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

TYPE_COLOUR = {
    "Person":  "#4C9BE8",
    "Company": "#F4A460",
    "City":    "#5DBB63",
    "Skill":   "#B088D4",
}
EDGE_COLOUR = {
    "KNOWS":      "#E05C5C",
    "WORKS_AT":   "#E0A020",
    "LIVES_IN":   "#40A8A0",
    "HAS_SKILL":  "#9060C0",
    "LOCATED_IN": "#60A060",
}
node_colours = [TYPE_COLOUR[G.nodes[n]["ntype"]] for n in G.nodes]
edge_colours = [EDGE_COLOUR[G.edges[e]["etype"]] for e in G.edges]

pos = nx.spring_layout(G, seed=42, k=2.2)

fig, ax = plt.subplots(figsize=(18, 12))
ax.set_facecolor("#F8F9FA")
fig.patch.set_facecolor("#F8F9FA")

nx.draw_networkx_edges(
    G, pos, ax=ax,
    edge_color=edge_colours, arrows=True, arrowsize=18, arrowstyle="-|>",
    width=1.8, alpha=0.75, connectionstyle="arc3,rad=0.08",
    min_source_margin=14, min_target_margin=14,
)
nx.draw_networkx_nodes(
    G, pos, ax=ax,
    node_color=node_colours, node_size=1600, alpha=0.95,
    linewidths=1.5, edgecolors="white",
)
nx.draw_networkx_labels(G, pos, ax=ax, font_size=8.5, font_weight="bold", font_color="white")

edge_labels = {(u, v): d["etype"] for u, v, d in G.edges(data=True)}
nx.draw_networkx_edge_labels(
    G, pos, edge_labels=edge_labels, ax=ax,
    font_size=6.5, font_color="#444444",
    bbox=dict(boxstyle="round,pad=0.15", fc="white", alpha=0.55, ec="none"),
    label_pos=0.35,
)

node_legend = [mpatches.Patch(color=c, label=lbl) for lbl, c in TYPE_COLOUR.items()]
edge_legend = [mpatches.Patch(color=c, label=lbl) for lbl, c in EDGE_COLOUR.items()]
leg1 = ax.legend(handles=node_legend, title="Node types",
                 loc="upper left", fontsize=9, title_fontsize=10,
                 framealpha=0.9, edgecolor="#CCCCCC")
ax.add_artist(leg1)
ax.legend(handles=edge_legend, title="Edge types",
          loc="lower left", fontsize=9, title_fontsize=10,
          framealpha=0.9, edgecolor="#CCCCCC")

ax.set_title("Social Network Graph — all vertices and edges", fontsize=15, fontweight="bold", pad=16)
ax.axis("off")
plt.tight_layout()

out_png = os.path.join(os.path.dirname(__file__), "..", "social_network_graph.png")
plt.savefig(out_png, dpi=150, bbox_inches="tight")
plt.close()
size_kb = os.path.getsize(out_png) // 1024
print(f"Static PNG saved: social_network_graph.png  ({size_kb} KB)")

# ── 3. Pyvis interactive HTML ─────────────────────────────────────────────────
from pyvis.network import Network

net = Network(
    height="620px", width="100%", directed=True,
    bgcolor="#F8F9FA", font_color="#222222",
    notebook=False, cdn_resources="in_line",
)

TYPE_COLOUR_PYVIS = TYPE_COLOUR.copy()

for node, data in G.nodes(data=True):
    ntype  = data["ntype"]
    colour = TYPE_COLOUR_PYVIS[ntype]
    shape  = {"Person": "ellipse", "Company": "box",
               "City": "diamond", "Skill": "triangle"}[ntype]
    net.add_node(
        node, label=node, title=f"<b>{node}</b><br>Type: {ntype}",
        color=colour, shape=shape, size=22,
        font={"size": 13, "bold": True},
    )

EDGE_COLOUR_PYVIS = EDGE_COLOUR.copy()

for u, v, data in G.edges(data=True):
    etype  = data["etype"]
    colour = EDGE_COLOUR_PYVIS[etype]
    net.add_edge(
        u, v, label=etype, title=etype,
        color=colour, width=2, arrows="to",
        font={"size": 10, "color": "#555555", "strokeWidth": 0},
    )

net.set_options("""{
  "physics": {
    "barnesHut": {
      "gravitationalConstant": -12000,
      "centralGravity": 0.25,
      "springLength": 160,
      "springConstant": 0.04,
      "damping": 0.12
    },
    "minVelocity": 0.75
  },
  "edges": { "smooth": { "type": "dynamic" } },
  "interaction": { "hover": true, "tooltipDelay": 100 }
}""")

out_html = os.path.join(os.path.dirname(__file__), "..", "social_network_interactive.html")
net.save_graph(out_html)
size_kb2 = os.path.getsize(out_html) // 1024
print(f"Interactive HTML saved: social_network_interactive.html  ({size_kb2} KB)")

print("\n✅  Smoke test passed — both outputs generated successfully")

