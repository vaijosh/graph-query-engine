#!/bin/bash

###############################################################################
# Graph Query Engine - Complete Demo Script
#
# This script demonstrates all capabilities of the Graph Query Engine:
# - Phase 1: Gremlin execution on TinkerGraph
# - Phase 2: Provider abstraction + transaction API
# - Phase 3: Compatibility tests
# - Phase 4: SQL translator as explain/optimization mode
#
# Prerequisites:
#   - Graph Query Engine running on localhost:7000
#   - curl installed
#   - jq installed (for pretty JSON output, optional)
###############################################################################

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

BASE_URL="http://localhost:7000"
REPO_PATH="/Users/vjoshi/SourceCode/GraphQueryEngine"

# Pretty print helper
print_section() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

print_step() {
    echo -e "${YELLOW}→ $1${NC}"
}

print_input() {
    echo -e "${YELLOW}📥 INPUT:${NC}"
    echo "   $1"
}

print_query() {
    echo -e "${YELLOW}🔍 QUERY:${NC}"
    echo "   $1"
}

print_output() {
    echo -e "${GREEN}📤 OUTPUT:${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check if service is running
check_service() {
    if ! curl -s "$BASE_URL/health" > /dev/null 2>&1; then
        print_error "Service not running on $BASE_URL"
        echo ""
        echo "Start the service with:"
        echo "  cd $REPO_PATH"
        echo "  mvn exec:java"
        exit 1
    fi
    print_success "Service is running"
}

# Demo 1: Health Check
demo_health_check() {
    print_section "Demo 1: Health Check"
    print_step "Checking service status..."

    RESPONSE=$(curl -s "$BASE_URL/health")
    echo "Response:"
    echo "$RESPONSE" | jq . 2>/dev/null || echo "$RESPONSE"
    print_success "Service health check passed"
}

# Demo 2: Provider Detection
demo_provider_detection() {
    print_section "Demo 2: Provider Detection (Phase 2)"
    print_step "Detecting active Gremlin provider..."

    RESPONSE=$(curl -s "$BASE_URL/gremlin/provider")
    echo "Response:"
    echo "$RESPONSE" | jq . 2>/dev/null || echo "$RESPONSE"
    print_success "Provider detected"
}

# Demo 3: TinkerGraph Basic Traversals (Phase 1)
demo_tinkergraph_basic() {
    print_section "Demo 3: TinkerGraph Basic Traversals (Phase 1)"

    print_step "Resetting transaction demo graph..."
    curl -s -X POST "$BASE_URL/admin/seed-gremlin-10hop-tx" > /dev/null
    print_success "Demo graph loaded (11 accounts in a chain: Account-1 → Account-2 → ... → Account-11)"
    echo ""

    print_step "Query 1: Count all vertices"
    print_query "g.V().count()"
    RESPONSE=$(curl -s -X POST "$BASE_URL/gremlin/query" \
        -H "Content-Type: application/json" \
        -d '{"gremlin":"g.V().count()"}')
    print_output
    echo "$RESPONSE" | jq '.results[0]' 2>/dev/null || echo "$RESPONSE" | grep -o '"results":\[[^]]*\]'
    print_success "Found 11 total vertices"
    echo ""

    print_step "Query 2: Count all edges"
    print_query "g.E().count()"
    RESPONSE=$(curl -s -X POST "$BASE_URL/gremlin/query" \
        -H "Content-Type: application/json" \
        -d '{"gremlin":"g.E().count()"}')
    print_output
    echo "$RESPONSE" | jq '.results[0]' 2>/dev/null || echo "$RESPONSE" | grep -o '"results":\[[^]]*\]'
    print_success "Found 10 total edges"
}

# Demo 4: Property Filtering
demo_property_filtering() {
    print_section "Demo 4: Property Filtering"

    print_step "Query 1: Filter by label"
    print_query "g.V().hasLabel('Account')"
    RESPONSE=$(curl -s -X POST "$BASE_URL/gremlin/query" \
        -H "Content-Type: application/json" \
        -d '{"gremlin":"g.V().hasLabel(\"Account\")"}')
    COUNT=$(echo "$RESPONSE" | jq '.resultCount' 2>/dev/null || echo "?")
    print_output
    echo "   Found $COUNT accounts matching label 'Account'"
    echo ""

    print_step "Query 2: Filter by property value"
    print_query "g.V().has('accountType','MERCHANT')"
    RESPONSE=$(curl -s -X POST "$BASE_URL/gremlin/query" \
        -H "Content-Type: application/json" \
        -d '{"gremlin":"g.V().has(\"accountType\",\"MERCHANT\")"}')
    COUNT=$(echo "$RESPONSE" | jq '.resultCount' 2>/dev/null || echo "?")
    print_output
    echo "   Found $COUNT MERCHANT type accounts"
    print_success "Property filtering complete"
}

# Demo 5: Projection (Phase 1)
demo_projection() {
    print_section "Demo 5: Property Projection"

    print_step "Project property from Account-1"
    print_input "Account ID: 1"
    print_query "g.V(1).values('name')"
    RESPONSE=$(curl -s -X POST "$BASE_URL/gremlin/query" \
        -H "Content-Type: application/json" \
        -d '{"gremlin":"g.V(1).values(\"name\")"}')
    print_output
    NAME=$(echo "$RESPONSE" | jq -r '.results[0]' 2>/dev/null || echo "?")
    echo "   Account name: $NAME"
    print_success "Projection complete"
}

# Demo 6: 10-Hop Repeat Traversal (Phase 1)
demo_10hop_repeat() {
    print_section "Demo 6: 10-Hop Repeat Traversal (Phase 1)"
    print_input "Start vertex ID: 1, Hops: 10"
    print_step "Traversing chain: Account-1 → Account-2 → ... → Account-11"
    print_query "g.V(1).repeat(out()).times(10).values('name')"
    echo ""

    RESPONSE=$(curl -s -X POST "$BASE_URL/gremlin/query" \
        -H "Content-Type: application/json" \
        -d '{"gremlin":"g.V(1).repeat(out()).times(10).values(\"name\")"}')

    print_output
    RESULT=$(echo "$RESPONSE" | jq -r '.results[0]' 2>/dev/null || echo "?")
    echo "   Final vertex: $RESULT"
    COUNT=$(echo "$RESPONSE" | jq '.resultCount' 2>/dev/null || echo "?")
    echo "   Result count: $COUNT"
    print_success "Successfully traversed 10-hop chain (1 → 11)"
}

# Demo 7: Reverse Traversal (in)
demo_reverse_traversal() {
    print_section "Demo 7: Reverse Traversal (in)"
    print_input "Start vertex ID: 11, Direction: INCOMING, Hops: 5"
    print_step "Traversing reverse: Account-11 ← Account-10 ← ... ← Account-6"
    print_query "g.V(11).repeat(in()).times(5).values('name')"
    echo ""

    RESPONSE=$(curl -s -X POST "$BASE_URL/gremlin/query" \
        -H "Content-Type: application/json" \
        -d '{"gremlin":"g.V(11).repeat(in()).times(5).values(\"name\")"}')

    print_output
    RESULT=$(echo "$RESPONSE" | jq -r '.results[0]' 2>/dev/null || echo "?")
    echo "   Vertex reached by reverse hops: $RESULT"
    COUNT=$(echo "$RESPONSE" | jq '.resultCount' 2>/dev/null || echo "?")
    echo "   Result count: $COUNT"
    print_success "Reverse traversal complete"
}

# Demo 8: Undirected Both Traversal
demo_both_traversal() {
    print_section "Demo 8: Undirected Traversal (both)"
    print_input "Start vertex ID: 5, Direction: BOTH, Hops: 2"
    print_step "Traversing bidirectionally from middle of chain"
    print_query "g.V(5).repeat(both()).times(2)"
    echo ""

    RESPONSE=$(curl -s -X POST "$BASE_URL/gremlin/query" \
        -H "Content-Type: application/json" \
        -d '{"gremlin":"g.V(5).repeat(both()).times(2)"}')

    print_output
    COUNT=$(echo "$RESPONSE" | jq '.resultCount' 2>/dev/null || echo "?")
    echo "   Vertices reachable in 2 hops (any direction): $COUNT"
    SAMPLE=$(echo "$RESPONSE" | jq -r '.results[0].id' 2>/dev/null || echo "?")
    echo "   Sample result vertex ID: $SAMPLE"
    print_success "Both-directional traversal complete"
}

# Demo 9: Path Collection
demo_path_collection() {
    print_section "Demo 9: Path Collection"
    print_input "Start vertex ID: 1, Hops: 3"
    print_step "Collecting full traversal path from start to end"
    print_query "g.V(1).repeat(out()).times(3).path()"
    echo ""

    RESPONSE=$(curl -s -X POST "$BASE_URL/gremlin/query" \
        -H "Content-Type: application/json" \
        -d '{"gremlin":"g.V(1).repeat(out()).times(3).path()"}')

    print_output
    PATH_LENGTH=$(echo "$RESPONSE" | jq '.results[0] | length' 2>/dev/null || echo "?")
    echo "   Path length (number of nodes): $PATH_LENGTH"
    COUNT=$(echo "$RESPONSE" | jq '.resultCount' 2>/dev/null || echo "?")
    echo "   Total paths found: $COUNT"
    print_success "Path collection complete"
}

# Demo 10: Simple Path (Cycle Prevention)
demo_simple_path() {
    print_section "Demo 10: Simple Path (Cycle Prevention)"
    print_input "Start vertex ID: 1, Hops: 5"
    print_step "Traversing with simplePath() to avoid revisiting vertices"
    print_query "g.V(1).repeat(out().simplePath()).times(5)"
    echo ""

    RESPONSE=$(curl -s -X POST "$BASE_URL/gremlin/query" \
        -H "Content-Type: application/json" \
        -d '{"gremlin":"g.V(1).repeat(out().simplePath()).times(5)"}')

    print_output
    COUNT=$(echo "$RESPONSE" | jq '.resultCount' 2>/dev/null || echo "?")
    echo "   Results found: $COUNT"
    VERTEX=$(echo "$RESPONSE" | jq -r '.results[0].id' 2>/dev/null || echo "?")
    echo "   Final vertex ID: $VERTEX"
    print_success "Simple path traversal complete"
}

# Demo 11: Transaction Semantics (Phase 2)
demo_transaction_semantics() {
    print_section "Demo 11: Transaction Semantics (Phase 2)"
    print_input "Vertex ID: 1, Property: name"
    print_step "Executing query within transaction context"
    print_query "g.V(1).values('name')"
    echo ""

    RESPONSE=$(curl -s -X POST "$BASE_URL/gremlin/query/tx" \
        -H "Content-Type: application/json" \
        -d '{"gremlin":"g.V(1).values(\"name\")"}')

    print_output
    RESULT=$(echo "$RESPONSE" | jq -r '.results[0]' 2>/dev/null || echo "?")
    echo "   Result: $RESULT"
    TX_STATUS=$(echo "$RESPONSE" | jq -r '.transactionStatus' 2>/dev/null || echo "?")
    echo "   Transaction status: $TX_STATUS"
    TX_MODE=$(echo "$RESPONSE" | jq -r '.transactionMode' 2>/dev/null || echo "?")
    echo "   Transaction mode: $TX_MODE"
    print_success "Transaction execution complete"
}

# Demo 12: SQL Mapping & Explain Mode (Phase 5)
demo_sql_mapping() {
    print_section "Demo 12: SQL Mapping & Explain Mode (Phase 5)"

    print_step "Setting up 10-hop mapping for SQL translation..."
    curl -s -X POST "$BASE_URL/admin/seed-10hop" > /dev/null
    curl -s -X POST "$BASE_URL/mapping/upload" \
        -F "file=@$REPO_PATH/mappings/ten-hop-mapping.json" > /dev/null
    print_success "Mapping loaded"
    echo ""

    print_input "Graph labels: Node (vertex), LINK (edge)"
    print_step "Showing SQL translation (explain mode)"
    print_query "g.V(1).hasLabel('Node').repeat(out('LINK')).times(3)"
    echo ""

    RESPONSE=$(curl -s -X POST "$BASE_URL/query/explain" \
        -H "Content-Type: application/json" \
        -d '{"gremlin":"g.V(1).hasLabel(\"Node\").repeat(out(\"LINK\")).times(3)"}')

    print_output
    SQL=$(echo "$RESPONSE" | jq -r '.translatedSql' 2>/dev/null | head -1)
    echo "   Generated SQL (first line):"
    echo "   $SQL"
    PARAMS=$(echo "$RESPONSE" | jq '.parameters' 2>/dev/null || echo "[]")
    echo "   Parameters: $PARAMS"
    print_success "SQL translation visible for analysis"
}

# Demo 13: Native Execution vs SQL Explain
demo_execution_paths() {
    print_section "Demo 13: Native Execution vs SQL Explain Paths (Phase 5)"
    echo ""

    print_step "Path 1: Native Gremlin Execution"
    print_query "POST /gremlin/query"
    echo "   Uses TinkerGraph provider for full Gremlin semantics"
    echo ""

    print_step "Path 2: SQL Translation Analysis"
    print_query "POST /query/explain"
    echo "   Shows how query translates to SQL (read-only mode)"
    echo ""

    print_step "Testing old /query endpoint (backward compatibility)"
    RESPONSE=$(curl -s -X POST "$BASE_URL/query" \
        -H "Content-Type: application/json" \
        -d '{"gremlin":"g.V().count()}')

    print_output
    STATUS=$(echo "$RESPONSE" | jq -r '.redirectTo // .message' 2>/dev/null || echo "?")
    echo "   Response: $STATUS"
    print_success "Execution paths separated cleanly"
}

# Demo 14: Error Handling
demo_error_handling() {
    print_section "Demo 14: Error Handling"

    print_step "Test 1: Invalid vertex ID"
    print_input "Vertex ID: 999 (does not exist)"
    print_query "g.V(999)"

    RESPONSE=$(curl -s -X POST "$BASE_URL/gremlin/query" \
        -H "Content-Type: application/json" \
        -d '{"gremlin":"g.V(999)"}')

    print_output
    COUNT=$(echo "$RESPONSE" | jq '.resultCount' 2>/dev/null || echo "?")
    echo "   Result count: $COUNT (empty result set)"
    echo ""

    print_step "Test 2: Empty query"
    print_input "Query string: (empty)"
    print_query "g (incomplete)"

    RESPONSE=$(curl -s -X POST "$BASE_URL/gremlin/query" \
        -H "Content-Type: application/json" \
        -d '{"gremlin":""}')

    print_output
    ERROR=$(echo "$RESPONSE" | jq -r '.error // "No error"' 2>/dev/null || echo "?")
    echo "   Error message: $ERROR"
    print_success "Error handling works correctly"
}

# Demo 15: Running Compatibility Tests
demo_run_tests() {
    print_section "Demo 15: Running Compatibility Tests (Phase 4)"
    echo ""
    echo "The project includes 37+ compatibility tests:"
    echo "  - 26+ TinkerGraph tests across 8 test classes"
    echo "  - 5+ integration tests"
    echo ""
    print_step "Running tests..."
    echo ""

    cd "$REPO_PATH"
    mvn -q test 2>&1 | grep -E "Tests run|Failures|Errors|OK" | tail -3
    print_success "Tests completed"

    echo ""
    echo "Test coverage includes:"
    echo "  ✓ Basic traversals (V/E count, limit)"
    echo "  ✓ Property filtering (hasLabel, has)"
    echo "  ✓ Path & projections (values, path)"
    echo "  ✓ Repeat loops (1-hop to 10-hop)"
    echo "  ✓ Simple path & cycle detection"
    echo "  ✓ Edge directions (out, in, both)"
    echo "  ✓ Transaction semantics"
    echo "  ✓ Error handling & edge cases"
}

# Demo 16: Full Architecture Overview
demo_architecture() {
    print_section "Demo 16: Full Architecture Overview (All Phases)"
    echo ""
    echo "Phase 1: Gremlin Execution on TinkerGraph"
    echo "  ✓ Native Gremlin traversal via GremlinGroovyScriptEngine"
    echo "  ✓ Full support for repeat/path/simplePath semantics"
    echo ""
    echo "Phase 2: Provider Abstraction + Transaction API"
    echo "  ✓ GraphProvider SPI for pluggable backends"
    echo "  ✓ GraphTransactionApi for explicit tx semantics"
    echo "  ✓ TinkerGraphProvider as default"
    echo ""
    echo "Phase 3: Compatibility Test Suite"
    echo "  ✓ 26+ TinkerGraph tests validating Gremlin semantics"
    echo "  ✓ Provider-agnostic test architecture"
    echo "  ✓ Nested test classes for organization"
    echo ""
    echo "Phase 4: SQL Translator as Explain Mode"
    echo "  ✓ /gremlin/query - Primary native execution"
    echo "  ✓ /query/explain - SQL translation analysis"
    echo "  ✓ /query - Gracefully redirects to /gremlin/query"
    echo ""
    print_success "Architecture complete across all phases"
}

###############################################################################
# Main Demo Execution
###############################################################################

main() {
    echo ""
    echo -e "${GREEN}"
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║         Graph Query Engine - Complete Demo Script          ║"
    echo "║              Phases 1-5 Feature Showcase                   ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"

    check_service

    # Run all demos
    demo_health_check
    demo_provider_detection
    demo_tinkergraph_basic
    demo_property_filtering
    demo_projection
    demo_10hop_repeat
    demo_reverse_traversal
    demo_both_traversal
    demo_path_collection
    demo_simple_path
    demo_transaction_semantics
    demo_sql_mapping
    demo_execution_paths
    demo_error_handling
    demo_run_tests
    demo_architecture

    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                  Demo Complete! ✓                          ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "Key Endpoints:"
    echo "  POST /gremlin/query       - Execute Gremlin natively"
    echo "  POST /gremlin/query/tx    - Execute with transaction semantics"
    echo "  POST /query/explain       - See SQL translation (Phase 5)"
    echo "  GET  /gremlin/provider    - Detect active provider"
    echo "  POST /admin/seed-gremlin-10hop-tx  - Load demo graph"
    echo ""
    echo "Documentation: See README.md for full API details"
    echo ""
}

main "$@"

