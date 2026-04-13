package com.graphqueryengine.gremlin;

import com.graphqueryengine.TestGraphH2;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.script.ScriptException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration-style tests for {@link GremlinExecutionService} using WCOJ + in-memory H2.
 *
 * <p>Replaces the former TinkerGraphProvider-based tests. The test graph is a
 * 10-hop Account chain seeded by {@link TestGraphH2}.
 */
class GremlinExecutionServiceTest {

    private static GremlinExecutionService svc;

    @BeforeAll
    static void setup() {
        svc = TestGraphH2.shared().wcojService();
    }

    @Test
    void executesTenHopRepeatOutQuery() throws ScriptException {
        GremlinExecutionResult result = svc.execute(
                "g.V().hasLabel('Account').has('txId','TXN-9001')" +
                ".repeat(out('TRANSFER')).times(10)" +
                ".project('accountId').by('accountId').limit(5)");

        assertEquals(1, result.resultCount());
        assertEquals("ACC-11", result.results().get(0));
    }

    @Test
    void executesPathQueryWithSimplePath() throws ScriptException {
        GremlinExecutionResult result = svc.execute(
                "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                ".repeat(out('TRANSFER').simplePath()).times(10)" +
                ".path().by('accountId').limit(5)");

        assertEquals(1, result.resultCount());
        assertInstanceOf(List.class, result.results().get(0));
    }

    @Test
    void executesQueryThroughTransactionApi() throws ScriptException {
        GremlinTransactionalExecutionResult result = svc.executeInTransaction(
                "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                ".repeat(out('TRANSFER')).times(10)" +
                ".project('accountId').by('accountId').limit(5)");

        assertEquals(1, result.resultCount());
        assertEquals("ACC-11", result.results().get(0));
        assertEquals("committed", result.transactionStatus());
    }

    @Test
    void executesTransactionalSumOnEdgeProperty() throws ScriptException {
        // WCOJ falls back to SQL for sum() queries
        GremlinTransactionalExecutionResult result = svc.executeInTransaction(
                "g.E().hasLabel('TRANSFER').values('amount').sum()");

        assertEquals(1, result.resultCount());
        assertInstanceOf(Number.class, result.results().get(0));
        // Edges have amounts 10,20,...,100 → sum = 550
        assertEquals(550.0, ((Number) result.results().get(0)).doubleValue(), 1e-6);
        assertEquals("committed", result.transactionStatus());
    }

    /**
     * Reproduces C1: project + outE count + order().by(select(),Order.desc).
     * SQL fallback path because project() is not WCOJ-eligible.
     */
    @Test
    void c1_projectOutDegreeOrderBySelectDesc() throws ScriptException {
        GremlinExecutionResult result = svc.execute(
                "g.V().hasLabel('Account')" +
                ".project('accountId','outDegree')" +
                ".by('accountId').by(outE('TRANSFER').count())" +
                ".order().by(select('outDegree'),Order.desc).limit(15)");

        // 11 accounts total, but only accounts 1–10 have outgoing TRANSFER edges
        assertTrue(result.resultCount() > 0, "Should return accounts with out-degree results");

        @SuppressWarnings("unchecked")
        Map<String, Object> first = (Map<String, Object>) result.results().get(0);
        // ACC-1..ACC-10 each have exactly 1 outgoing edge; ACC-11 has 0
        assertNotNull(first.get("accountId"));
        assertNotNull(first.get("outDegree"));
    }
}
