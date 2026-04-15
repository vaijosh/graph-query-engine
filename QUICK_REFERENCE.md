# 🚀 Quick Reference - AML Demo Notebook

## One-Minute Setup

### Option A — Python-first (start service + load data from the notebook)
```python
# Cell 1 — in the notebook
from demo.demo_app import DemoApp
app = DemoApp()
app.start()     # starts the Java service as a subprocess, waits until healthy
app.load_csv()  # uploads AML mapping + loads demo/data/aml-demo.csv
```

### Option B — Manual (separate terminal)
```bash
# Terminal 1: Start service
cd /Users/vjoshi/SourceCode/GraphQueryEngine
mvn exec:java

# Terminal 2: Start Jupyter
cd /Users/vjoshi/SourceCode/GraphQueryEngine
jupyter notebook demo/aml/notebooks/aml_sql_showcase.ipynb
```

**Browser automatically opens** → Run cells top-to-bottom

---

## Command Cheat Sheet

| Task | Command |
|------|---------|
| **Install dependencies** | `pip install jupyter pandas requests ipython` |
| **Start service (Python)** | `python3 demo/demo_app.py` |
| **Start service (Maven)** | `mvn exec:java` |
| **Load CSV (Python)** | `python3 demo/aml_csv_loader.py` |
| **Normalize CSV** | `python3 scripts/normalize_aml.py` |
| **Launch notebook** | `jupyter notebook demo/aml/notebooks/aml_sql_showcase.ipynb` |
| **Run all cells** | In notebook: Kernel → Restart & Run All |
| **Run single cell** | Click cell + press Shift+Enter |
| **Stop service** | Ctrl+C in service terminal |
| **View notebook locally** | `http://localhost:8888` |

---

## Notebook Cell Execution Order

```
1. Setup & Configuration         ← Run first
2. Helper Functions              ← Required
3. Health Check                  ← Verify service
4. CSV Loading                   ← Load data
5. S1-S8 (Simple Queries)       ← Optional
6. C1-C11 (Complex Queries)     ← Optional
7. Summary                       ← Reference
```

---

## Minimal Working Example

```python
# After running Setup, Health Check, and CSV Loading cells...

# Run a simple query:
gremlin = "g.V().count()"
result = run_gremlin_query(gremlin)
display_query_result(gremlin, result, title="Total Accounts")

# Expected output: 81352 (or similar)
```

---

## Simple Query Reference (S1-S8)

```groovy
// S1: Total accounts
g.V().count()

// S2: Total transfers
g.E().count()

// S3: Suspicious count
g.E().has('isLaundering','1').count()

// S4: Suspicious details
g.E().has('isLaundering','1')
  .project('from','to','amount','currency','timestamp')
  .by(outV().values('accountId'))
  .by(inV().values('accountId'))
  .by('amount')
  .by('currency')
  .by('eventTime')

// S5: Currency distribution
g.E().groupCount().by('currency')

// S6: Format distribution
g.E().groupCount().by('paymentFormat')

// S7: Suspicious by currency
g.E().has('isLaundering','1').groupCount().by('currency')

// S8: Suspicious by format
g.E().has('isLaundering','1').groupCount().by('paymentFormat')
```

---

## Complex Query Reference (C1-C11)

```groovy
// C1: Top hub accounts
g.V().project('accountId','bankId','outDegree')
  .by('accountId')
  .by('bankId')
  .by(outE('TRANSFER').count())
  .order().by(select('outDegree'),Order.desc)
  .limit(15)

// C2: Suspicious hubs
g.V().project('accountId','bankId','suspiciousOut','totalOut')
  .by('accountId')
  .by('bankId')
  .by(outE('TRANSFER').has('isLaundering','1').count())
  .by(outE('TRANSFER').count())
  .where(select('suspiciousOut').is(gt(0)))
  .order().by(select('suspiciousOut'),Order.desc)
  .limit(15)

// C3: Cross-bank suspicious flow
g.E().has('isLaundering','1').as('e')
  .project('fromBank','fromAcct','toBank','toAcct','amount')
  .by(outV().values('bankId'))
  .by(outV().values('accountId'))
  .by(inV().values('bankId'))
  .by(inV().values('accountId'))
  .by('amount')
  .limit(15)

// C4: 2-hop suspicious chains
g.E().has('isLaundering','1').outV().as('a')
  .outE('TRANSFER').has('isLaundering','1').inV().as('b')
  .where('a',neq('b'))
  .select('a','b').by('accountId')
  .limit(20)

// C5: 1-hop neighbor network
g.V().where(outE('TRANSFER').has('isLaundering','1'))
  .limit(1).as('hub')
  .both('TRANSFER').dedup()
  .project('accountId','bankId')
  .by('accountId')
  .by('bankId')
  .limit(20)

// C6: 3-hop paths
g.V().where(outE('TRANSFER').has('isLaundering','1'))
  .limit(1)
  .repeat(out('TRANSFER').simplePath()).times(3)
  .path().by('accountId')
  .limit(20)

// C7: 5-hop paths
g.V().where(outE('TRANSFER').has('isLaundering','1'))
  .limit(1)
  .repeat(out('TRANSFER').simplePath()).times(5)
  .path().by('accountId')
  .limit(10)

// C8: 10-hop investigation range
g.V().where(outE('TRANSFER').has('isLaundering','1'))
  .limit(1)
  .repeat(out('TRANSFER').simplePath()).times(10)
  .path().by('accountId')
  .limit(5)

// C9: 2-hop reachability
g.V().where(outE('TRANSFER').has('isLaundering','1'))
  .limit(1)
  .repeat(out('TRANSFER')).times(2)
  .dedup().count()

// C10: Suspicious count
g.E().has('isLaundering','1').count()

// C11: SQL explain (optional)
g.V(1).hasLabel("Node").repeat(out("LINK")).times(3)
```

---

## Troubleshooting Quick Fixes

| Problem | Solution |
|---------|----------|
| Connection refused | `mvn exec:java` not running |
| CSV not found | Run: `python3 scripts/normalize_aml.py` |
| Empty results | Restart Jupyter: Kernel → Restart Kernel |
| Timeout error | Add `.limit(20)` to query |
| Service slow | Reduce `MAX_ROWS` in Setup cell |

---

## File Locations

```
/Users/vjoshi/SourceCode/GraphQueryEngine/
├── demo/aml/notebooks/
│   └── aml_sql_showcase.ipynb          ← Main notebook
├── JUPYTER_NOTEBOOK_GUIDE.md           ← Full guide
├── demo/data/
│   ├── aml-demo.csv                    ← Normalized (generated)
│   ├── HI-Small_Trans.csv              ← Raw data
│   └── ...
└── scripts/
    └── normalize_aml.py                ← Normalization script
```

---

## Common Customizations

### Use Different CSV File
```python
CSV_PATH = str(Path.cwd() / "demo/data/HI-Large_Trans.csv")
MAX_ROWS = 500000
```

### Connect to Remote Service
```python
BASE_URL = "http://192.168.1.100:7000"
```

### Show More Results
```python
display_query_result(gremlin, result, title="...", limit=50)
```

### Create Custom Query
```python
my_gremlin = "g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(5)"
result = run_gremlin_query(my_gremlin)
display_query_result(my_gremlin, result, title="My Query")
```

---

## Expected Output Examples

### Count Query (S1)
```
Result Count: 1
[81352]
```

### Projection Query (C1)
```
accountId | bankId | outDegree
A001      | B123   | 45
A002      | B456   | 38
A003      | B789   | 32
```

### Path Query (C6)
```
Result Count: 20
[['ACC-1', 'ACC-2', 'ACC-3', 'ACC-4']]
[['ACC-5', 'ACC-6', 'ACC-7', 'ACC-8']]
...
```

### Group Query (S5)
```
Currency | Count
USD      | 45000
EUR      | 30000
GBP      | 25000
```

---

## Performance Tips

- **Keep limits realistic**: `.limit(20)` not `.limit(1000000)`
- **Reduce hops for speed**: `times(3)` faster than `times(10)`
- **Filter early**: Add `.has()` before expensive operations
- **Use dedup()**: Reduces result set size with `.repeat()`
- **Start small**: Use HI-Small_Trans.csv first, then scale up

---

## Success Criteria

✅ Jupyter opens  
✅ Health Check passes  
✅ CSV loads successfully  
✅ S1 returns > 0  
✅ C1 returns 15 results  
✅ All cells run without errors  

**You're ready!** 🎉

---

**For detailed info, see: JUPYTER_NOTEBOOK_GUIDE.md**

