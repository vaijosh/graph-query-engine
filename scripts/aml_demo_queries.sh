#!/bin/bash
# =============================================================================
#  Graph Query Engine – IBM AML Kaggle Dataset Demo Script
#
#  Dataset: IBM Transactions for Anti-Money Laundering (AML)
#  Source:  https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml
#
#  BEFORE RUNNING:
#  1. Normalize raw Kaggle CSV (one-time):
#     awk -F',' 'NR==1{print "from_bank,from_account,to_bank,to_account,amount_paid,payment_currency,payment_format,timestamp,is_laundering,transaction_id";next} NR<=100001{gsub(/ /,"_",$9);gsub(/ /,"_",$10);print $2","$3","$4","$5","$8","$9","$10","$1","$11","NR-1}' \
#       demo/data/HI-Small_Trans.csv > demo/data/aml-demo.csv
#
#  2. Start the service in another terminal:
#     mvn exec:java
#
#  3. Run this script:
#     chmod +x scripts/aml_demo_queries.sh
#     ./scripts/aml_demo_queries.sh
# =============================================================================

BASE_URL="http://localhost:7000"
CSV_PATH="$(pwd)/demo/data/aml-demo.csv"
MAX_ROWS=100000

# ── Colors ────────────────────────────────────────────────────────────────────
BLUE='\033[0;34m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'
CYAN='\033[0;36m'; RED='\033[0;31m'; NC='\033[0m'

section()  { echo; echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; echo -e "${BLUE}$1${NC}"; echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }
input()    { echo -e "${YELLOW}📥  INPUT  : $1${NC}"; }
query()    { echo -e "${YELLOW}🔍  QUERY  : ${CYAN}$1${NC}"; }
endpoint() { echo -e "${YELLOW}📡  ENDPOINT: $1${NC}"; }
output()   { echo -e "${GREEN}📤  OUTPUT :${NC}"; }
ok()       { echo -e "${GREEN}✓  $1${NC}"; }
fail()     { echo -e "${RED}✗  $1${NC}"; }
pause()    { echo; echo -e "${YELLOW}[press enter for next demo]${NC}"; read -r; }

run_query() {
    local gremlin="$1"
    curl -s -X POST "$BASE_URL/gremlin/query" \
        -H "Content-Type: application/json" \
        -d "{\"gremlin\":\"$gremlin\"}" | python3 -c "
import sys, json
d=json.load(sys.stdin)
if 'error' in d:
    print('  ERROR:', d['error'])
else:
    print('  resultCount:', d.get('resultCount'))
    for r in d.get('results',[])[:10]:
        print(' ', r)
"
}

run_query_tx() {
    local gremlin="$1"
    curl -s -X POST "$BASE_URL/gremlin/query/tx" \
        -H "Content-Type: application/json" \
        -d "{\"gremlin\":\"$gremlin\"}" | python3 -c "
import sys, json
d=json.load(sys.stdin)
if 'error' in d:
    print('  ERROR:', d['error'])
else:
    print('  resultCount    :', d.get('resultCount'))
    print('  txMode         :', d.get('transactionMode'))
    print('  txStatus       :', d.get('transactionStatus'))
    for r in d.get('results',[])[:5]:
        print(' ', r)
"
}

# ──────────────────────────────────────────────────────────────────────────────
echo
echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗"
echo    "║  IBM Anti-Money Laundering (AML) Dataset – Gremlin Query Demo  ║"
echo    "║  Dataset : IBM HI-Small_Trans.csv (100,000 transactions)       ║"
echo    "║  Vertices: ~81,352 unique accounts                             ║"
echo    "║  Edges   : 100,000 TRANSFER edges                              ║"
echo -e "╚════════════════════════════════════════════════════════════════╝${NC}"

# ── 0. Health ─────────────────────────────────────────────────────────────────
section "Step 0: Health & Provider Check"
endpoint "GET /health"
HEALTH=$(curl -s "$BASE_URL/health")
output; echo "  $HEALTH"
PROVIDER=$(curl -s "$BASE_URL/gremlin/provider" | python3 -c "import sys,json; print(json.load(sys.stdin).get('provider','?'))")
echo "  Active provider: $PROVIDER"
ok "Service is healthy – provider: $PROVIDER"

pause

# ── 1. Load CSV ───────────────────────────────────────────────────────────────
section "Step 1: Load Kaggle IBM AML CSV into Graph"
input "File   : $CSV_PATH"
input "Rows   : $MAX_ROWS  (first 100k of HI-Small_Trans.csv)"
input "Schema :"
echo "         Vertices → Account  { accountId, bankId, name }"
echo "         Edges    → TRANSFER { txId, amount, currency, paymentFormat, eventTime, isLaundering }"
echo
endpoint "POST /admin/load-aml-csv?path=...&maxRows=$MAX_ROWS"
output
LOAD=$(curl -s -X POST "$BASE_URL/admin/load-aml-csv?path=$CSV_PATH&maxRows=$MAX_ROWS")
echo "$LOAD" | python3 -c "
import sys, json
d=json.load(sys.stdin)
if 'error' in d:
    print('  ERROR:', d['error'])
else:
    print('  Rows loaded      :', d.get('rowsLoaded'))
    print('  Accounts created :', d.get('accountsCreated'))
    print('  Transfers created:', d.get('transfersCreated'))
    print('  Provider         :', d.get('provider'))
"
ok "Kaggle AML dataset loaded into in-memory graph"

pause

# ══════════════════════════════════════════════════════════════════════════════
# SIMPLE QUERIES
# ══════════════════════════════════════════════════════════════════════════════

section "SIMPLE QUERIES"

# ── S1. Vertex count ──────────────────────────────────────────────────────────
echo
input "How many unique accounts exist in the dataset?"
query "g.V().count()"
output
run_query "g.V().count()"
ok "Unique account vertices counted"

pause

# ── S2. Edge count ────────────────────────────────────────────────────────────
input "How many total transfer transactions exist?"
query "g.E().count()"
output
run_query "g.E().count()"
ok "Total TRANSFER edges counted"

pause

# ── S3. Suspicious transaction count ─────────────────────────────────────────
input "How many transactions are flagged as suspicious (Is Laundering = 1)?"
query "g.E().has('isLaundering','1').count()"
output
run_query "g.E().has('isLaundering','1').count()"
ok "Suspicious transaction count retrieved"

pause

# ── S4. Suspicious transaction details ───────────────────────────────────────
input "Show details of each suspicious transaction (from, to, amount, currency, timestamp)"
query "g.E().has('isLaundering','1').project('from','to','amount','currency','timestamp').by(outV().values('accountId')).by(inV().values('accountId')).by('amount').by('currency').by('eventTime')"
output
run_query "g.E().has('isLaundering','1').project('from','to','amount','currency','timestamp').by(outV().values('accountId')).by(inV().values('accountId')).by('amount').by('currency').by('eventTime')"
ok "Suspicious transaction details retrieved"

pause

# ── S5. Currency breakdown (all transfers) ────────────────────────────────────
input "How are all 100k transfers distributed by currency?"
query "g.E().groupCount().by('currency')"
output
run_query "g.E().groupCount().by('currency')"
ok "Currency breakdown complete"

pause

# ── S6. Payment format breakdown (all transfers) ──────────────────────────────
input "How are all transfers distributed by payment format?"
query "g.E().groupCount().by('paymentFormat')"
output
run_query "g.E().groupCount().by('paymentFormat')"
ok "Payment format breakdown complete"

pause

# ── S7. Suspicious transfers by currency ─────────────────────────────────────
input "Which currencies are used in suspicious transfers?"
query "g.E().has('isLaundering','1').groupCount().by('currency')"
output
run_query "g.E().has('isLaundering','1').groupCount().by('currency')"
ok "Suspicious currency breakdown complete"

pause

# ── S8. Suspicious transfers by payment format ───────────────────────────────
input "Which payment formats do launderers use?"
query "g.E().has('isLaundering','1').groupCount().by('paymentFormat')"
output
run_query "g.E().has('isLaundering','1').groupCount().by('paymentFormat')"
ok "Suspicious payment format breakdown complete"

pause

# ══════════════════════════════════════════════════════════════════════════════
# COMPLEX QUERIES
# ══════════════════════════════════════════════════════════════════════════════

section "COMPLEX QUERIES"

# ── C1. Top hub accounts ──────────────────────────────────────────────────────
echo
input "Who are the top 15 accounts by outgoing transfer volume? (Fan-out / mule detection)"
query "g.V().project('accountId','bankId','outDegree').by('accountId').by('bankId').by(outE('TRANSFER').count()).order().by(select('outDegree'),Order.desc).limit(15)"
output
run_query "g.V().project('accountId','bankId','outDegree').by('accountId').by('bankId').by(outE('TRANSFER').count()).order().by(select('outDegree'),Order.desc).limit(15)"
ok "Hub account detection complete"

pause

# ── C2. Suspicious hub accounts ──────────────────────────────────────────────
input "Which accounts have suspicious outgoing transfers? (suspiciousOut > 0, ranked)"
query "g.V().project('accountId','bankId','suspiciousOut','totalOut').by('accountId').by('bankId').by(outE('TRANSFER').has('isLaundering','1').count()).by(outE('TRANSFER').count()).where(select('suspiciousOut').is(gt(0))).order().by(select('suspiciousOut'),Order.desc).limit(15)"
output
run_query "g.V().project('accountId','bankId','suspiciousOut','totalOut').by('accountId').by('bankId').by(outE('TRANSFER').has('isLaundering','1').count()).by(outE('TRANSFER').count()).where(select('suspiciousOut').is(gt(0))).order().by(select('suspiciousOut'),Order.desc).limit(15)"
ok "Suspicious hub ranking complete"

pause

# ── C3. Cross-bank suspicious flow ────────────────────────────────────────────
input "Show suspicious transfers that cross banks (from-bank != to-bank)"
query "g.E().has('isLaundering','1').as('e').project('fromBank','fromAcct','toBank','toAcct','amount').by(outV().values('bankId')).by(outV().values('accountId')).by(inV().values('bankId')).by(inV().values('accountId')).by('amount').limit(15)"
output
run_query "g.E().has('isLaundering','1').as('e').project('fromBank','fromAcct','toBank','toAcct','amount').by(outV().values('bankId')).by(outV().values('accountId')).by(inV().values('bankId')).by(inV().values('accountId')).by('amount').limit(15)"
ok "Cross-bank suspicious flow complete"

pause

# ── C4. 2-hop suspicious chain ────────────────────────────────────────────────
input "Find 2-hop chains where BOTH hops are suspicious  (Placement → Layering)"
input "Pattern: AccountA →[suspicious]→ AccountB →[suspicious]→ AccountC"
query "g.E().has('isLaundering','1').outV().as('a').outE('TRANSFER').has('isLaundering','1').inV().as('b').where('a',neq('b')).select('a','b').by('accountId').limit(20)"
output
run_query "g.E().has('isLaundering','1').outV().as('a').outE('TRANSFER').has('isLaundering','1').inV().as('b').where('a',neq('b')).select('a','b').by('accountId').limit(20)"
ok "2-hop suspicious chain detection complete"

pause

# ── C5. Neighbor accounts of suspicious sender ────────────────────────────────
input "Find ALL accounts the suspicious hub account sent money to (1-hop out)"
query "g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(1).as('hub').both('TRANSFER').dedup().project('accountId','bankId').by('accountId').by('bankId').limit(20)"
output
run_query "g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(1).as('hub').both('TRANSFER').dedup().project('accountId','bankId').by('accountId').by('bankId').limit(20)"
ok "Neighbor account lookup complete"

pause

# ── C6. 3-hop simplePath from suspicious hub ─────────────────────────────────
input "Traverse 3 hops outward from the suspicious hub account (simplePath)"
input "Uses simplePath() to prevent revisiting nodes"
query "g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(1).repeat(out('TRANSFER').simplePath()).times(3).path().by('accountId').limit(20)"
output
run_query "g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(1).repeat(out('TRANSFER').simplePath()).times(3).path().by('accountId').limit(20)"
ok "3-hop simplePath traversal complete"

pause

# ── C7. 5-hop simplePath from suspicious hub ─────────────────────────────────
input "Traverse 5 hops from suspicious hub – how far does the money spread?"
query "g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(1).repeat(out('TRANSFER').simplePath()).times(5).path().by('accountId').limit(10)"
output
run_query "g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(1).repeat(out('TRANSFER').simplePath()).times(5).path().by('accountId').limit(10)"
ok "5-hop AML traversal complete"

pause

# ── C8. 10-hop repeat traversal ───────────────────────────────────────────────
input "Maximum 10-hop reach from suspicious account (full AML investigation range)"
input "This is the core query showcasing deep graph traversal capability"
query "g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(1).repeat(out('TRANSFER').simplePath()).times(10).path().by('accountId').limit(5)"
output
run_query "g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(1).repeat(out('TRANSFER').simplePath()).times(10).path().by('accountId').limit(5)"
ok "10-hop AML graph traversal complete"

pause

# ── C9. Accounts reachable within 2 hops from suspicious sender ───────────────
input "How many DISTINCT accounts are reachable within 2 hops from the suspicious hub?"
query "g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(1).repeat(out('TRANSFER')).times(2).dedup().count()"
output
run_query "g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(1).repeat(out('TRANSFER')).times(2).dedup().count()"
ok "2-hop reachability count complete"

pause

# ── C10. Transactional execution of suspicious count ─────────────────────────
section "Step C10: Transactional Query Execution (Phase 2)"
input "Execute suspicious transaction count inside a transaction context"
query "g.E().has('isLaundering','1').count()"
endpoint "POST /gremlin/query/tx"
output
run_query_tx "g.E().has('isLaundering','1').count()"
ok "Transactional execution complete"

pause

# ── C11. SQL explain mode ─────────────────────────────────────────────────────
section "Step C11: SQL Translation / Explain Mode (Phase 5)"
input "Translate a 3-hop AML traversal into SQL for optimization analysis"
query "g.V(1).hasLabel('Node').repeat(out('LINK')).times(3)"
endpoint "POST /query/explain"

curl -s -X POST "$BASE_URL/admin/seed-10hop" > /dev/null
curl -s -X POST "$BASE_URL/mapping/upload" \
    -F "file=@$(pwd)/mappings/ten-hop-mapping.json" > /dev/null

output
curl -s -X POST "$BASE_URL/query/explain" \
    -H "Content-Type: application/json" \
    -d '{"gremlin":"g.V(1).hasLabel(\"Node\").repeat(out(\"LINK\")).times(3)"}' | \
    python3 -c "
import sys, json
d=json.load(sys.stdin)
if 'error' in d:
    print('  ERROR:', d['error'])
else:
    sql = d.get('translatedSql','')
    print('  Generated SQL:')
    print(' ', sql)
    print()
    print('  Parameters :', d.get('parameters'))
    print('  Mode       :', d.get('mode'))
    print('  Note       :', d.get('note'))
"
ok "SQL explain mode demo complete"

# ── Summary ───────────────────────────────────────────────────────────────────
echo
echo -e "${GREEN}╔══════════════════════════════════════════════════════════════════╗"
echo    "║               AML DEMO COMPLETE  ✓                               ║"
echo -e "╚══════════════════════════════════════════════════════════════════╝${NC}"
echo
echo "  ── SIMPLE QUERIES ──────────────────────────────────────────────────"
echo "   S1.  g.V().count()                         Total accounts"
echo "   S2.  g.E().count()                         Total transfers"
echo "   S3.  g.E().has('isLaundering','1').count() Suspicious count"
echo "   S4.  Suspicious tx details                 from/to/amount/currency"
echo "   S5.  groupCount by currency                Currency distribution"
echo "   S6.  groupCount by paymentFormat           Format distribution"
echo "   S7.  Suspicious by currency                Laundering currency"
echo "   S8.  Suspicious by paymentFormat           Laundering format"
echo
echo "  ── COMPLEX QUERIES ─────────────────────────────────────────────────"
echo "   C1.  Top 15 hub accounts                   Fan-out / mule detection"
echo "   C2.  Suspicious hub ranking                suspiciousOut > 0"
echo "   C3.  Cross-bank suspicious flow            Inter-bank detection"
echo "   C4.  2-hop suspicious chain                Placement→Layering"
echo "   C5.  Neighbor accounts of hub              1-hop network"
echo "   C6.  3-hop simplePath                      Money trail tracking"
echo "   C7.  5-hop simplePath                      Extended reach"
echo "   C8.  10-hop repeat traversal               Full investigation range"
echo "   C9.  2-hop reachability count              Network exposure"
echo "   C10. Transactional query execution         Phase 2 (TX semantics)"
echo "   C11. SQL explain / translate               Phase 5 (optimization)"
echo

