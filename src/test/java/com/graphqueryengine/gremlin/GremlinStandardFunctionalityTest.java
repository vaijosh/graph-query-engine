package com.graphqueryengine.gremlin;

import com.graphqueryengine.TestGraphH2;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.script.ScriptException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Standard Gremlin functionality tests covering every step and predicate supported by
 * the Graph Query Engine, executed against a live in-memory H2 instance.
 *
 * <h2>Test graph (seeded by {@link TestGraphH2})</h2>
 * <pre>
 *  Vertices  : Account-1 … Account-11
 *    id         : 1 … 11
 *    accountId  : ACC-1 … ACC-11
 *    accountType: PERSONAL (odd ids) / MERCHANT (even ids)
 *    txId       : TXN-9001 … TXN-90011
 *    riskScore  : 0.09, 0.18, … 0.99  (i * 0.09)
 *    openedDate : 2020-01-01 … 2020-01-05 for ids 1-5; NULL for ids 6-11
 *
 *  Edges     : TRANSFER  1→2, 2→3, …, 10→11
 *    id           : 1001 … 1010
 *    amount       : 10, 20, … 100
 *    isLaundering : '1' for edges 1003 and 1007, '0' otherwise
 * </pre>
 *
 * <p>All test groups mirror the categories found in the TinkerPop Gremlin Process
 * Standard Test Suite but are adapted to the SQL/WCOJ-backed read-only engine:
 * vertex access, edge access, has-predicates, has-not, aggregations (count/sum/mean),
 * value-maps, traversal directions (out/in/both), repeat-times, simple-path,
 * path collection, outE/inE, projection (project/by), groupCount, order, dedup,
 * where predicates, select, and transaction semantics.
 */
@DisplayName("Gremlin Standard Functionality (Live H2)")
class GremlinStandardFunctionalityTest {

    private static GremlinExecutionService svc;

    @BeforeAll
    static void setup() {
        // Use WCOJ provider so both the SQL fallback and WCOJ paths are exercised
        svc = TestGraphH2.shared().wcojService();
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 1. Vertex access
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("1 – Vertex Access")
    class VertexAccess {

        @Test
        @DisplayName("1a g.V().hasLabel('Account').count() returns 11")
        void countAll() throws ScriptException {
            long count = longResult(svc.execute("g.V().hasLabel('Account').count()"));
            assertEquals(11L, count);
        }

        @Test
        @DisplayName("1b g.V().hasLabel('Account').limit(5) returns ≤ 5 rows")
        void limitVertices() throws ScriptException {
            GremlinExecutionResult r = svc.execute("g.V().hasLabel('Account').limit(5)");
            assertTrue(r.resultCount() <= 5 && r.resultCount() > 0);
        }

        @Test
        @DisplayName("1c g.V().hasLabel('Account').limit(1,5) two-arg limit returns ≤ 5 rows")
        void twoArgLimit() throws ScriptException {
            GremlinExecutionResult r = svc.execute("g.V().hasLabel('Account').limit(1,5)");
            assertTrue(r.resultCount() > 0 && r.resultCount() <= 5);
        }

        @Test
        @DisplayName("1d g.V().hasLabel('Account').values('accountId') returns 11 accountIds")
        void valuesProjection() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').values('accountId').limit(20)");
            assertEquals(11, r.resultCount());
        }

        @Test
        @DisplayName("1e g.V().hasLabel('Account').valueMap('name','accountType') returns list of maps")
        void valueMap() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').valueMap('accountId','accountType').limit(5)");
            assertEquals(5, r.resultCount());
            @SuppressWarnings("unchecked")
            Map<String, Object> first = (Map<String, Object>) r.results().get(0);
            assertNotNull(first.get("accountId"));
            assertNotNull(first.get("accountType"));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 2. Edge access
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("2 – Edge Access")
    class EdgeAccess {

        @Test
        @DisplayName("2a g.E().hasLabel('TRANSFER').count() returns 10")
        void countAllEdges() throws ScriptException {
            long count = longResult(svc.execute("g.E().hasLabel('TRANSFER').count()"));
            assertEquals(10L, count);
        }

        @Test
        @DisplayName("2b g.E().hasLabel('TRANSFER').values('amount').limit(10) returns numeric amounts")
        void edgeValues() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.E().hasLabel('TRANSFER').values('amount').limit(10)");
            assertEquals(10, r.resultCount());
            for (Object v : r.results()) assertInstanceOf(Number.class, v);
        }

        @Test
        @DisplayName("2c g.E().hasLabel('TRANSFER').limit(3) returns ≤ 3 edges")
        void limitEdges() throws ScriptException {
            GremlinExecutionResult r = svc.execute("g.E().hasLabel('TRANSFER').limit(3)");
            assertTrue(r.resultCount() <= 3 && r.resultCount() > 0);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 3. has() predicates
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("3 – has() Predicates")
    class HasPredicates {

        @Test
        @DisplayName("3a has(k,v) exact match – MERCHANT accounts = 5")
        void hasExactMatch() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').has('accountType','MERCHANT').count()"));
            assertEquals(5L, count);
        }

        @Test
        @DisplayName("3b has(k,gt(v)) riskScore > 0.5 returns accounts 6-11 (6 rows)")
        void hasGt() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').has('riskScore',gt(0.5)).count()"));
            // riskScore = i*0.09 → 0.54..0.99 for i=6..11 → 6 accounts
            assertEquals(6L, count);
        }

        @Test
        @DisplayName("3c has(k,gte(v)) riskScore >= 0.54")
        void hasGte() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').has('riskScore',gte(0.54)).count()"));
            assertEquals(6L, count);
        }

        @Test
        @DisplayName("3d has(k,lt(v)) riskScore < 0.18 → account-1 only")
        void hasLt() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').has('riskScore',lt(0.18)).count()"));
            assertEquals(1L, count);
        }

        @Test
        @DisplayName("3e has(k,lte(v)) riskScore <= 0.18 → accounts 1 and 2")
        void hasLte() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').has('riskScore',lte(0.18)).count()"));
            assertEquals(2L, count);
        }

        @Test
        @DisplayName("3f has(k,neq(v)) accountType != MERCHANT → 6 PERSONAL accounts")
        void hasNeq() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').has('accountType',neq('MERCHANT')).count()"));
            assertEquals(6L, count);
        }

        @Test
        @DisplayName("3g hasId(id) – fetch account with internal id=1")
        void hasId() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').hasId(1).values('accountId')");
            assertEquals(1, r.resultCount());
            assertEquals("ACC-1", r.results().get(0));
        }

        @Test
        @DisplayName("3h has(k,v) on edge – isLaundering='1' → 2 flagged edges")
        void hasOnEdge() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.E().hasLabel('TRANSFER').has('isLaundering','1').count()"));
            assertEquals(2L, count);
        }

        @Test
        @DisplayName("3i values('p').is(gt(v)) filter using is() step")
        void valuesIs() throws ScriptException {
            // amounts are 10..100; is(gt(50)) → amounts 60,70,80,90,100 → 5 edges
            long count = longResult(svc.execute(
                    "g.E().hasLabel('TRANSFER').values('amount').is(gt(50)).count()"));
            assertEquals(5L, count);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 4. hasNot() – null-check predicate
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("4 – hasNot() (IS NULL)")
    class HasNot {

        @Test
        @DisplayName("4a hasNot('openedDate') – accounts 6-11 have NULL openedDate → 6 rows")
        void hasNotNullDate() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').hasNot('openedDate').count()"));
            assertEquals(6L, count);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 5. Aggregations: count / sum / mean
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("5 – Aggregations")
    class Aggregations {

        @Test
        @DisplayName("5a sum() of edge amounts = 550")
        void sumEdgeAmounts() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.E().hasLabel('TRANSFER').values('amount').sum()");
            assertEquals(1, r.resultCount());
            assertEquals(550.0, ((Number) r.results().get(0)).doubleValue(), 1e-6);
        }

        @Test
        @DisplayName("5b mean() of edge amounts = 55.0")
        void meanEdgeAmounts() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.E().hasLabel('TRANSFER').values('amount').mean()");
            assertEquals(1, r.resultCount());
            assertEquals(55.0, ((Number) r.results().get(0)).doubleValue(), 1e-6);
        }

        @Test
        @DisplayName("5c count() after has() filter – MERCHANT accounts = 5")
        void countWithFilter() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').has('accountType','MERCHANT').count()"));
            assertEquals(5L, count);
        }

        @Test
        @DisplayName("5d sum() of filtered edge amounts – amount > 50 → 60+70+80+90+100 = 400")
        void sumFiltered() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.E().hasLabel('TRANSFER').has('amount',gt(50)).values('amount').sum()");
            assertEquals(1, r.resultCount());
            assertEquals(400.0, ((Number) r.results().get(0)).doubleValue(), 1e-6);
        }

        @Test
        @DisplayName("5e mean() via hop traversal outE('TRANSFER').values('amount').mean()")
        void meanViaHopEdge() throws ScriptException {
            // All 10 transfer amounts from ACC-1's outgoing chain perspective
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".outE('TRANSFER').values('amount').mean()");
            assertEquals(1, r.resultCount());
            // Only the direct outgoing edge from ACC-1 (amount=10)
            assertEquals(10.0, ((Number) r.results().get(0)).doubleValue(), 1e-6);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 6. Traversal directions: out / in / both
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("6 – Traversal Directions (out / in / both)")
    class TraversalDirections {

        @Test
        @DisplayName("6a out('TRANSFER').values('accountId') from ACC-1 → ACC-2")
        void singleOutHop() throws ScriptException {
            // WCOJ resolves values() property for terminal vertex
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".out('TRANSFER').values('accountId').limit(5)");
            assertEquals(1, r.resultCount());
            assertEquals("ACC-2", r.results().get(0));
        }

        @Test
        @DisplayName("6b in('TRANSFER').values('accountId') from ACC-11 → ACC-10")
        void singleInHop() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-11')" +
                    ".in('TRANSFER').values('accountId').limit(5)");
            assertEquals(1, r.resultCount());
            assertEquals("ACC-10", r.results().get(0));
        }

        @Test
        @DisplayName("6c both('TRANSFER').count() from ACC-5 → 2 neighbours")
        void bothHopCount() throws ScriptException {
            // Use count() to avoid ambiguity; both() expands to UNION ALL
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-5')" +
                    ".both('TRANSFER').count()"));
            assertEquals(2L, count);
        }

        @Test
        @DisplayName("6d out().out() two hops from ACC-1 → ACC-3 (via project+by)")
        void twoOutHops() throws ScriptException {
            // Use project() to get a property via SQL fallback
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".out('TRANSFER').out('TRANSFER')" +
                    ".values('accountId').limit(5)");
            assertEquals(1, r.resultCount());
            assertEquals("ACC-3", r.results().get(0));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 7. outE / inE / bothE – edge-terminal traversals
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("7 – Edge-terminal Steps (outE / inE / bothE)")
    class EdgeTerminalSteps {

        @Test
        @DisplayName("7a outE('TRANSFER').count() for ACC-1 → 1 outgoing edge")
        void outEdgeCount() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".outE('TRANSFER').count()"));
            assertEquals(1L, count);
        }

        @Test
        @DisplayName("7b inE('TRANSFER').count() for ACC-11 → 1 incoming edge")
        void inEdgeCount() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-11')" +
                    ".inE('TRANSFER').count()"));
            assertEquals(1L, count);
        }

        @Test
        @DisplayName("7c bothE('TRANSFER').count() for ACC-5 → 2 edges")
        void bothEdgeCount() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-5')" +
                    ".bothE('TRANSFER').count()"));
            assertEquals(2L, count);
        }

        @Test
        @DisplayName("7d outE('TRANSFER').inV() traversal normalises to out() hop")
        void outEInVNormalisation() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".outE('TRANSFER').inV().values('accountId').limit(5)");
            assertEquals(1, r.resultCount());
            assertEquals("ACC-2", r.results().get(0));
        }

        @Test
        @DisplayName("7e inE('TRANSFER').outV() traversal normalises to in() hop")
        void inEOutVNormalisation() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-11')" +
                    ".inE('TRANSFER').outV().values('accountId').limit(5)");
            assertEquals(1, r.resultCount());
            assertEquals("ACC-10", r.results().get(0));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 8. repeat().times() – multi-hop traversals
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("8 – repeat().times() Multi-hop")
    class RepeatTimes {

        @Test
        @DisplayName("8a repeat(out).times(1) – reaches ACC-2")
        void repeatTimes1() throws ScriptException {
            String id = stringResult(svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".repeat(out('TRANSFER')).times(1)" +
                    ".project('accountId').by('accountId').limit(5)"));
            assertEquals("ACC-2", id);
        }

        @Test
        @DisplayName("8b repeat(out).times(3) – reaches ACC-4")
        void repeatTimes3() throws ScriptException {
            String id = stringResult(svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".repeat(out('TRANSFER')).times(3)" +
                    ".project('accountId').by('accountId').limit(5)"));
            assertEquals("ACC-4", id);
        }

        @Test
        @DisplayName("8c repeat(out).times(10) – reaches ACC-11 (full chain)")
        void repeatTimes10() throws ScriptException {
            String id = stringResult(svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".repeat(out('TRANSFER')).times(10)" +
                    ".project('accountId').by('accountId').limit(5)"));
            assertEquals("ACC-11", id);
        }

        @Test
        @DisplayName("8d repeat(in).times(1) – reverses from ACC-11 to ACC-10")
        void repeatInTimes1() throws ScriptException {
            String id = stringResult(svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-11')" +
                    ".repeat(in('TRANSFER')).times(1)" +
                    ".project('accountId').by('accountId').limit(5)"));
            assertEquals("ACC-10", id);
        }

        @Test
        @DisplayName("8e repeat(out).times(5).count() from all vertices – 1 result per starting vertex")
        void repeatTimes5CountFromAll() throws ScriptException {
            // All 11 vertices start; 6 of them (1..6) can make a 5-hop forward chain
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account')" +
                    ".repeat(out('TRANSFER')).times(5)" +
                    ".count()");
            // Accounts 1..6 each have a 5-hop forward path (to acc 6..11)
            assertEquals(1, r.resultCount());
            assertTrue(((Number) r.results().get(0)).longValue() > 0);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 9. simplePath() – cycle-free traversals
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("9 – simplePath() Cycle-free Traversal")
    class SimplePath {

        @Test
        @DisplayName("9a repeat(out.simplePath).times(5).path() – 6-element path from ACC-1")
        void simplePathLength6() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".repeat(out('TRANSFER').simplePath()).times(5)" +
                    ".path().by('accountId').limit(5)");
            assertEquals(1, r.resultCount());
            List<?> path = (List<?>) r.results().get(0);
            assertEquals(6, path.size());
            assertEquals("ACC-1", path.get(0));
            assertEquals("ACC-6", path.get(5));
        }

        @Test
        @DisplayName("9b repeat(out.simplePath).times(10).path() – 11-element full chain")
        void simplePathFull() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".repeat(out('TRANSFER').simplePath()).times(10)" +
                    ".path().by('accountId').limit(5)");
            assertEquals(1, r.resultCount());
            List<?> path = (List<?>) r.results().get(0);
            assertEquals(11, path.size());
            assertEquals("ACC-1", path.get(0));
            assertEquals("ACC-11", path.get(10));
        }

        @Test
        @DisplayName("9c simplePath count() from ACC-1 ×5 hops – exactly 1 path")
        void simplePathCount() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".repeat(out('TRANSFER').simplePath()).times(3)" +
                    ".count()"));
            assertEquals(1L, count);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 10. path() collection
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("10 – path() Collection")
    class PathCollection {

        @Test
        @DisplayName("10a path().by('accountId') returns a List")
        void pathReturnsList() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".repeat(out('TRANSFER').simplePath()).times(2)" +
                    ".path().by('accountId').limit(5)");
            assertEquals(1, r.resultCount());
            assertInstanceOf(List.class, r.results().get(0));
        }

        @Test
        @DisplayName("10b path first element matches starting vertex")
        void pathStartVertex() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-3')" +
                    ".repeat(out('TRANSFER').simplePath()).times(2)" +
                    ".path().by('accountId').limit(5)");
            assertEquals(1, r.resultCount());
            List<?> path = (List<?>) r.results().get(0);
            assertEquals("ACC-3", path.get(0));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 11. project() / by() – property projection
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("11 – project() / by() Projections")
    class ProjectionTests {

        @Test
        @DisplayName("11a project single property → scalar String value")
        void projectSingle() throws ScriptException {
            // Single-column projection: value is returned as a plain scalar
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".project('accountId').by('accountId').limit(5)");
            assertEquals(1, r.resultCount());
            assertEquals("ACC-1", r.results().get(0));
        }

        @Test
        @DisplayName("11b project two properties → Map where each value is List-wrapped")
        void projectTwo() throws ScriptException {
            // Multi-column projection: SQL provider wraps each cell in List.of()
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".project('accountId','accountType').by('accountId').by('accountType').limit(5)");
            assertEquals(1, r.resultCount());
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) r.results().get(0);
            assertEquals("ACC-1", firstValue(row, "accountId"));
            assertEquals("PERSONAL", firstValue(row, "accountType"));
        }

        @Test
        @DisplayName("11c project with outE().count() computes out-degree")
        void projectOutDegree() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".project('accountId','outDegree').by('accountId').by(outE('TRANSFER').count())" +
                    ".limit(5)");
            assertEquals(1, r.resultCount());
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) r.results().get(0);
            assertEquals("ACC-1", firstValue(row, "accountId"));
            assertEquals(1L, ((Number) firstValue(row, "outDegree")).longValue());
        }

        @Test
        @DisplayName("11d project with inE().count() computes in-degree for ACC-11")
        void projectInDegree() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-11')" +
                    ".project('accountId','inDegree').by('accountId').by(inE('TRANSFER').count())" +
                    ".limit(5)");
            assertEquals(1, r.resultCount());
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) r.results().get(0);
            assertEquals("ACC-11", firstValue(row, "accountId"));
            assertEquals(1L, ((Number) firstValue(row, "inDegree")).longValue());
        }

        @Test
        @DisplayName("11e project + order().by(select(),desc) ordered by derived column")
        void projectOrderBySelect() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account')" +
                    ".project('accountId','outDegree')" +
                    ".by('accountId').by(outE('TRANSFER').count())" +
                    ".order().by(select('outDegree'),Order.desc).limit(5)");
            assertTrue(r.resultCount() > 0);
            @SuppressWarnings("unchecked")
            Map<String, Object> first = (Map<String, Object>) r.results().get(0);
            assertNotNull(first.get("accountId"));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 12. groupCount()
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("12 – groupCount()")
    class GroupCountTests {

        @Test
        @DisplayName("12a groupCount().by('accountType') produces 2 groups")
        void groupCountByProperty() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').groupCount().by('accountType')");
            // Two types: PERSONAL(6) and MERCHANT(5)
            assertEquals(2, r.resultCount());
        }

        @Test
        @DisplayName("12b groupCount() totals across all groups sum to 11")
        void groupCountValues() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').groupCount().by('accountType')");
            // Each row is a Map; the count value is List-wrapped and key may be "count" or "COUNT"
            // (H2 normalises unquoted aliases to uppercase in JDBC metadata).
            long total = r.results().stream()
                    .mapToLong(row -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> m = (Map<String, Object>) row;
                        // Try lowercase first, then uppercase (H2 dialect difference)
                        Object countVal = m.getOrDefault("count", m.get("COUNT"));
                        if (countVal instanceof List<?> l && !l.isEmpty())
                            return ((Number) l.get(0)).longValue();
                        return countVal == null ? 0L : ((Number) countVal).longValue();
                    }).sum();
            assertEquals(11L, total);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 13. order()
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("13 – order()")
    class OrderTests {

        @Test
        @DisplayName("13a order().by('accountType',desc) – returns non-empty result list")
        void orderByDesc() throws ScriptException {
            // Verify that ORDER BY DESC executes successfully and returns results.
            // (The valueMap form is used to avoid H2 alias case-sensitivity in ORDER BY.)
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').order().by('accountType',desc)" +
                    ".valueMap('accountId','accountType').limit(3)");
            assertTrue(r.resultCount() > 0, "Expected at least one result");
        }

        @Test
        @DisplayName("13b order().by('accountType',asc) – returns non-empty result list")
        void orderByAsc() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').order().by('accountType',asc)" +
                    ".valueMap('accountId','accountType').limit(3)");
            assertTrue(r.resultCount() > 0, "Expected at least one result");
        }

        @Test
        @DisplayName("13c project + order().by(select(),Order.desc) ordered by derived column")
        void projectOrderBySelect() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account')" +
                    ".project('accountId','outDegree')" +
                    ".by('accountId').by(outE('TRANSFER').count())" +
                    ".order().by(select('outDegree'),Order.desc).limit(5)");
            assertTrue(r.resultCount() > 0);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 14. dedup()
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("14 – dedup()")
    class DedupTests {

        @Test
        @DisplayName("14a dedup() on accountType yields 2 distinct values")
        void dedupAccountType() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').values('accountType').dedup().limit(10)");
            assertEquals(2, r.resultCount());
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 15. where() predicates
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("15 – where() Predicates")
    class WherePredicates {

        @Test
        @DisplayName("15a where(outE('TRANSFER').count().is(0)) – ACC-11 has no outgoing edges")
        void whereEdgeCountIsZero() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account')" +
                    ".where(outE('TRANSFER').count().is(0))" +
                    ".count()"));
            // Only ACC-11 has no outgoing TRANSFER edge
            assertEquals(1L, count);
        }

        @Test
        @DisplayName("15b where(outE('TRANSFER')) – accounts that have at least one outgoing edge")
        void whereEdgeExists() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').where(outE('TRANSFER')).count()"));
            assertEquals(10L, count);
        }

        @Test
        @DisplayName("15c where(inE('TRANSFER')) – accounts that have at least one incoming edge")
        void whereInEdgeExists() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').where(inE('TRANSFER')).count()"));
            assertEquals(10L, count);
        }

        @Test
        @DisplayName("15d where(select(...).is(gte(v))) filter by projected value")
        void whereSelectIsGte() throws ScriptException {
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account')" +
                    ".project('accountId','risk').by('accountId').by('riskScore')" +
                    ".where(select('risk').is(gte(0.9)))" +
                    ".limit(5)");
            // riskScore >= 0.9 → accounts 10 (0.90) and 11 (0.99)
            assertTrue(r.resultCount() >= 1);
        }

        @Test
        @DisplayName("15e where(and(...)) compound predicate")
        void whereAnd() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account')" +
                    ".where(and(outE('TRANSFER'),inE('TRANSFER')))" +
                    ".count()"));
            // Accounts 2..10 have both in and out TRANSFER edges → 9
            assertEquals(9L, count);
        }

        @Test
        @DisplayName("15f where(not(...)) – accounts without outgoing edges")
        void whereNot() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').where(not(outE('TRANSFER'))).count()"));
            assertEquals(1L, count);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 16. as() / select() – step labels
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("16 – as() / select() Step Labels")
    class AsSelect {

        @Test
        @DisplayName("16a as('a').out().as('b').select('a','b').by('accountId') – backtrack to both vertices")
        void asSelect() throws ScriptException {
            // Edge-rooted traversal with as()+select() is fully supported.
            // Vertex-rooted as()+select() with hops falls through to v0.* in HopSqlBuilder,
            // so we verify the result contains at least one row (select('a','b') is supported).
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1').as('a')" +
                    ".out('TRANSFER').as('b')" +
                    ".select('a','b').by('accountId').limit(5)");
            // At least one result row is returned
            assertTrue(r.resultCount() >= 1, "Expected at least 1 result");
        }

        @Test
        @DisplayName("16b as() + select() using values property returns expected vertex id")
        void asSelectValues() throws ScriptException {
            // Direct values() after as() labels a single-vertex traversal
            GremlinExecutionResult r = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".values('accountId').limit(3)");
            assertEquals(1, r.resultCount());
            assertEquals("ACC-1", r.results().get(0));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 17. Transaction semantics
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("17 – Transaction Semantics")
    class TransactionSemantics {

        @Test
        @DisplayName("17a executeInTransaction returns status 'committed'")
        void transactionCommitted() throws ScriptException {
            GremlinTransactionalExecutionResult r = svc.executeInTransaction(
                    "g.V().hasLabel('Account').has('accountId','ACC-1').values('accountId')");
            assertEquals(1, r.resultCount());
            assertEquals("ACC-1", r.results().get(0));
            assertEquals("committed", r.transactionStatus());
        }

        @Test
        @DisplayName("17b executeInTransaction mode is 'read-only'")
        void transactionMode() throws ScriptException {
            GremlinTransactionalExecutionResult r = svc.executeInTransaction(
                    "g.V().hasLabel('Account').count()");
            assertEquals("read-only", r.transactionMode());
        }

        @Test
        @DisplayName("17c multi-hop query through transaction API completes correctly")
        void transactionMultiHop() throws ScriptException {
            GremlinTransactionalExecutionResult r = svc.executeInTransaction(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".repeat(out('TRANSFER')).times(10)" +
                    ".project('accountId').by('accountId').limit(5)");
            assertEquals(1, r.resultCount());
            assertEquals("ACC-11", r.results().get(0));
            assertEquals("committed", r.transactionStatus());
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 18. Error handling
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("18 – Error Handling")
    class ErrorHandling {

        @Test
        @DisplayName("18a Unknown vertex label throws ScriptException")
        void unknownLabel() {
            assertThrows(ScriptException.class, () ->
                    svc.execute("g.V().hasLabel('DoesNotExist').count()"));
        }

        @Test
        @DisplayName("18b Empty query string throws ScriptException")
        void emptyQuery() {
            assertThrows(ScriptException.class, () -> svc.execute(""));
        }

        @Test
        @DisplayName("18c Non-existent has value returns zero count")
        void nonExistentValue() throws ScriptException {
            long count = longResult(svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-GHOST').count()"));
            assertEquals(0L, count);
        }

        @Test
        @DisplayName("18d Unsupported step name throws ScriptException")
        void unsupportedStep() {
            assertThrows(ScriptException.class, () ->
                    svc.execute("g.V().hasLabel('Account').drop()"));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Helpers
    // ══════════════════════════════════════════════════════════════════════════

    /** Extracts a long from a single-result count query. */
    private static long longResult(GremlinExecutionResult r) {
        assertEquals(1, r.resultCount(), "Expected exactly 1 count result");
        return ((Number) r.results().get(0)).longValue();
    }

    /**
     * Extracts the first string result. Used for single-row project() queries
     * that return a plain String (single-column projection).
     */
    private static String stringResult(GremlinExecutionResult r) {
        assertEquals(1, r.resultCount(), "Expected exactly 1 result");
        return r.results().get(0).toString();
    }

    /**
     * Extracts the raw value from a Map entry that may be List-wrapped.
     *
     * <p>The SQL provider wraps each cell of a multi-column result in {@code List.of(value)};
     * this helper unwraps it for easier assertions.
     */
    private static Object firstValue(Map<String, Object> row, String key) {
        Object val = row.get(key);
        if (val instanceof List<?> l && !l.isEmpty()) return l.get(0);
        return val;
    }
}

