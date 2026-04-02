package com.graphqueryengine.gremlin;

import com.graphqueryengine.gremlin.provider.GraphProvider;
import com.graphqueryengine.gremlin.provider.TinkerGraphProvider;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.script.ScriptException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Gremlin Compatibility Tests (Curated Subset)")
class GremlinCompatibilityTest {

    private GraphProvider provider;

    private static void seedTenHopChain(TinkerGraphProvider provider) {
        Vertex previous = null;
        for (int i = 1; i <= 11; i++) {
            Vertex current = provider.getGraph().addVertex(
                    T.id, i,
                    T.label, "Account",
                    "name", "Acct-" + i,
                    "accountType", (i % 2 == 0) ? "MERCHANT" : "PERSONAL",
                    "txId", "TXN-900" + i
            );
            if (previous != null) {
                previous.addEdge("TRANSFER", current, T.id, 1000L + i, "amount", 10.0 * i);
            }
            previous = current;
        }
    }

    @BeforeEach
    void setup() {
        TinkerGraphProvider tinkerProvider = new TinkerGraphProvider();
        seedTenHopChain(tinkerProvider);
        provider = tinkerProvider;
    }

    @Nested
    @DisplayName("Basic Vertex/Edge Traversals")
    class BasicTraversals {

        @Test
        @DisplayName("g.V().count() - Count all vertices")
        void countAllVertices() throws ScriptException {
            GremlinExecutionResult result = provider.execute("g.V().count()");
            assertEquals(1, result.resultCount());
            assertEquals(11L, result.results().get(0));
        }

        @Test
        @DisplayName("g.E().count() - Count all edges")
        void countAllEdges() throws ScriptException {
            GremlinExecutionResult result = provider.execute("g.E().count()");
            assertEquals(1, result.resultCount());
            assertEquals(10L, result.results().get(0));
        }

        @Test
        @DisplayName("g.V().limit(3) - Get first 3 vertices")
        void limitVertices() throws ScriptException {
            GremlinExecutionResult result = provider.execute("g.V().limit(3)");
            assertEquals(3, result.resultCount());
        }
    }

    @Nested
    @DisplayName("Property Filtering (has)")
    class PropertyFiltering {

        @Test
        @DisplayName("g.V().hasLabel('Account') - Filter by label")
        void filterByLabel() throws ScriptException {
            GremlinExecutionResult result = provider.execute("g.V().hasLabel('Account')");
            assertTrue(result.resultCount() > 0);
            assertEquals(11, result.resultCount());
        }

        @Test
        @DisplayName("g.V().has('accountType','MERCHANT') - Filter by property value")
        void filterByPropertyValue() throws ScriptException {
            GremlinExecutionResult result = provider.execute("g.V().has('accountType','MERCHANT')");
            assertTrue(result.resultCount() > 0);
            assertEquals(5, result.resultCount()); // Half of 10 accounts (ids 2, 4, 6, 8, 10)
        }
    }

    @Nested
    @DisplayName("Path & Projection Traversals")
    class PathAndProjections {

        @Test
        @DisplayName("g.V(1).values('name') - Project single property")
        void projectProperty() throws ScriptException {
            GremlinExecutionResult result = provider.execute("g.V(1).values('name')");
            assertEquals(1, result.resultCount());
            assertEquals("Acct-1", result.results().get(0));
        }

        @Test
        @DisplayName("g.V().path() - Collect traversal paths")
        void collectPaths() throws ScriptException {
            GremlinExecutionResult result = provider.execute("g.V(1).repeat(out()).times(2).path()");
            assertEquals(1, result.resultCount());
            assertInstanceOf(List.class, result.results().get(0));
        }
    }

    @Nested
    @DisplayName("Repeat & Loop Traversals")
    class RepeatTraversals {

        @Test
        @DisplayName("g.V(1).repeat(out()).times(1) - Single hop")
        void singleHopRepeat() throws ScriptException {
            GremlinExecutionResult result = provider.execute("g.V(1).repeat(out()).times(1)");
            assertEquals(1, result.resultCount());
        }

        @Test
        @DisplayName("g.V(1).repeat(out()).times(3) - Three hops")
        void threeHopRepeat() throws ScriptException {
            GremlinExecutionResult result = provider.execute("g.V(1).repeat(out()).times(3)");
            assertEquals(1, result.resultCount());
        }

        @Test
        @DisplayName("g.V(1).repeat(out()).times(10) - Max chain hops (1->11)")
        void tenHopRepeat() throws ScriptException {
            GremlinExecutionResult result = provider.execute("g.V(1).repeat(out()).times(10)");
            assertEquals(1, result.resultCount());
            @SuppressWarnings("unchecked")
            Map<String, Object> vertex = (Map<String, Object>) result.results().get(0);
            assertEquals(11, vertex.get("id"));
        }
    }

    @Nested
    @DisplayName("Simple Path & Cycle Detection")
    class SimplePathTraversals {

        @Test
        @DisplayName("g.V(1).repeat(out().simplePath()).times(5) - Avoid cycles")
        void simplePathRepeat() throws ScriptException {
            GremlinExecutionResult result = provider.execute("g.V(1).repeat(out().simplePath()).times(5)");
            assertEquals(1, result.resultCount());
        }

        @Test
        @DisplayName("g.V().cyclicPath() - Find cycles (if any)")
        void cyclicPath() throws ScriptException {
            // Chain graph has no cycles
            GremlinExecutionResult result = provider.execute("g.V().cyclicPath()");
            assertEquals(0, result.resultCount());
        }
    }

    @Nested
    @DisplayName("Edge Traversal Direction")
    class EdgeDirection {

        @Test
        @DisplayName("g.V(11).repeat(in()).times(1) - Reverse single hop")
        void inboundHop() throws ScriptException {
            GremlinExecutionResult result = provider.execute("g.V(11).repeat(in()).times(1)");
            assertEquals(1, result.resultCount());
            @SuppressWarnings("unchecked")
            Map<String, Object> vertex = (Map<String, Object>) result.results().get(0);
            assertEquals(10, vertex.get("id"));
        }

        @Test
        @DisplayName("g.V(1).repeat(both()).times(1) - Both directions")
        void bothDirections() throws ScriptException {
            GremlinExecutionResult result = provider.execute("g.V(1).repeat(both()).times(1)");
            assertEquals(1, result.resultCount());
        }
    }


    @Nested
    @DisplayName("Transaction Semantics")
    class TransactionSemantics {

        @Test
        @DisplayName("execute in transaction - returns committed status")
        void transactionExecution() throws ScriptException {
            GremlinTransactionalExecutionResult result = provider.executeInTransaction(
                    "g.V(1).values('name')"
            );
            assertEquals(1, result.resultCount());
            assertEquals("Acct-1", result.results().get(0));
            assertEquals("EXECUTED", result.transactionStatus());
        }
    }

    @Nested
    @DisplayName("Error Handling & Edge Cases")
    class ErrorHandling {

        @Test
        @DisplayName("Invalid vertex ID returns empty result")
        void invalidVertexId() throws ScriptException {
            GremlinExecutionResult result = provider.execute("g.V(999)");
            assertEquals(0, result.resultCount());
        }

        @Test
        @DisplayName("Empty Gremlin query throws error")
        void emptyQuery() {
            org.junit.jupiter.api.Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () -> provider.execute("")
            );
        }
    }
}

