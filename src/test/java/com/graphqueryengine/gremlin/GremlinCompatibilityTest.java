package com.graphqueryengine.gremlin;

import com.graphqueryengine.TestGraphH2;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.script.ScriptException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Gremlin compatibility tests using WCOJ + in-memory H2.
 *
 * <p>Replaces the former TinkerGraphProvider-backed suite. The test graph is a
 * 10-hop Account chain seeded by {@link TestGraphH2}:
 * Account-1 → Account-2 → … → Account-11 (TRANSFER edges).
 */
@DisplayName("Gremlin Compatibility Tests (WCOJ + H2)")
class GremlinCompatibilityTest {

    private static GremlinExecutionService svc;

    @BeforeAll
    static void setup() {
        svc = TestGraphH2.shared().wcojService();
    }

    @Nested
    @DisplayName("Basic Vertex/Edge Traversals")
    class BasicTraversals {

        @Test
        @DisplayName("g.V().hasLabel('Account').count() - Count all vertices")
        void countAllVertices() throws ScriptException {
            GremlinExecutionResult result = svc.execute("g.V().hasLabel('Account').count()");
            assertEquals(1, result.resultCount());
            assertEquals(11L, ((Number) result.results().get(0)).longValue());
        }

        @Test
        @DisplayName("g.V().hasLabel('Account').limit(3) - Get first 3 vertices")
        void limitVertices() throws ScriptException {
            GremlinExecutionResult result = svc.execute("g.V().hasLabel('Account').limit(3)");
            assertTrue(result.resultCount() > 0);
            assertTrue(result.resultCount() <= 3);
        }
    }

    @Nested
    @DisplayName("Property Filtering (has)")
    class PropertyFiltering {

        @Test
        @DisplayName("g.V().hasLabel('Account') - Count via label")
        void filterByLabel() throws ScriptException {
            GremlinExecutionResult result = svc.execute("g.V().hasLabel('Account').count()");
            assertEquals(11L, ((Number) result.results().get(0)).longValue());
        }

        @Test
        @DisplayName("g.V().hasLabel('Account').has('accountType','MERCHANT') - Filter by property value")
        void filterByPropertyValue() throws ScriptException {
            GremlinExecutionResult result = svc.execute(
                    "g.V().hasLabel('Account').has('accountType','MERCHANT').count()");
            // Accounts 2,4,6,8,10 are MERCHANT → 5
            assertEquals(5L, ((Number) result.results().get(0)).longValue());
        }
    }

    @Nested
    @DisplayName("Path & Projection Traversals")
    class PathAndProjections {

        @Test
        @DisplayName("g.V().has('accountId','ACC-1').values('accountId') - Project single property")
        void projectProperty() throws ScriptException {
            GremlinExecutionResult result = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1').values('accountId')");
            assertEquals(1, result.resultCount());
            assertEquals("ACC-1", result.results().get(0));
        }

        @Test
        @DisplayName("repeat(out).times(2).path() - Collect 2-hop traversal paths")
        void collectPaths() throws ScriptException {
            GremlinExecutionResult result = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".repeat(out('TRANSFER').simplePath()).times(2)" +
                    ".path().by('accountId').limit(5)");
            assertEquals(1, result.resultCount());
            assertInstanceOf(List.class, result.results().get(0));
        }
    }

    @Nested
    @DisplayName("Repeat & Loop Traversals")
    class RepeatTraversals {

        @Test
        @DisplayName("repeat(out).times(1) - Single hop")
        void singleHopRepeat() throws ScriptException {
            GremlinExecutionResult result = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".repeat(out('TRANSFER')).times(1)" +
                    ".project('accountId').by('accountId').limit(5)");
            assertEquals(1, result.resultCount());
            assertEquals("ACC-2", result.results().get(0));
        }

        @Test
        @DisplayName("repeat(out).times(3) - Three hops")
        void threeHopRepeat() throws ScriptException {
            GremlinExecutionResult result = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".repeat(out('TRANSFER')).times(3)" +
                    ".project('accountId').by('accountId').limit(5)");
            assertEquals(1, result.resultCount());
            assertEquals("ACC-4", result.results().get(0));
        }

        @Test
        @DisplayName("repeat(out).times(10) - Max chain hops (ACC-1→ACC-11)")
        void tenHopRepeat() throws ScriptException {
            GremlinExecutionResult result = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".repeat(out('TRANSFER')).times(10)" +
                    ".project('accountId').by('accountId').limit(5)");
            assertEquals(1, result.resultCount());
            assertEquals("ACC-11", result.results().get(0));
        }
    }

    @Nested
    @DisplayName("Simple Path Traversals")
    class SimplePathTraversals {

        @Test
        @DisplayName("repeat(out.simplePath).times(5) - Avoid revisiting vertices")
        void simplePathRepeat() throws ScriptException {
            GremlinExecutionResult result = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-1')" +
                    ".repeat(out('TRANSFER').simplePath()).times(5)" +
                    ".path().by('accountId').limit(5)");
            assertEquals(1, result.resultCount());
            // 5 hops from ACC-1 → path has 6 entries (v0..v5)
            List<?> path = (List<?>) result.results().get(0);
            assertEquals(6, path.size());
        }
    }

    @Nested
    @DisplayName("Edge Traversal Direction")
    class EdgeDirection {

        @Test
        @DisplayName("repeat(in).times(1) - Reverse single hop from ACC-11")
        void inboundHop() throws ScriptException {
            GremlinExecutionResult result = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-11')" +
                    ".repeat(in('TRANSFER')).times(1)" +
                    ".project('accountId').by('accountId').limit(5)");
            assertEquals(1, result.resultCount());
            assertEquals("ACC-10", result.results().get(0));
        }
    }

    @Nested
    @DisplayName("Transaction Semantics")
    class TransactionSemantics {

        @Test
        @DisplayName("executeInTransaction returns committed status")
        void transactionExecution() throws ScriptException {
            GremlinTransactionalExecutionResult result = svc.executeInTransaction(
                    "g.V().hasLabel('Account').has('accountId','ACC-1').values('accountId')");
            assertEquals(1, result.resultCount());
            assertEquals("ACC-1", result.results().get(0));
            assertEquals("committed", result.transactionStatus());
        }
    }

    @Nested
    @DisplayName("Error Handling & Edge Cases")
    class ErrorHandling {

        @Test
        @DisplayName("Non-existent property value returns zero count")
        void unknownPropertyValue() throws ScriptException {
            GremlinExecutionResult result = svc.execute(
                    "g.V().hasLabel('Account').has('accountId','ACC-DOES-NOT-EXIST').count()");
            assertEquals(0L, ((Number) result.results().get(0)).longValue());
        }

        @Test
        @DisplayName("Empty Gremlin query throws ScriptException")
        void emptyQuery() {
            assertThrows(ScriptException.class, () -> svc.execute(""));
        }
    }
}

