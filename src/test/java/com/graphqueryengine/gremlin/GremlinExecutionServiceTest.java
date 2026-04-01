package com.graphqueryengine.gremlin;

import com.graphqueryengine.gremlin.provider.TinkerGraphProvider;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.jupiter.api.Test;

import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GremlinExecutionServiceTest {

    private GremlinExecutionService newSeededTenHopService() {
        TinkerGraphProvider provider = new TinkerGraphProvider();
        TinkerGraph graph = provider.getGraph();

        Vertex previous = null;
        for (int i = 1; i <= 11; i++) {
            Vertex current = graph.addVertex(
                    T.id, i,
                    T.label, "Account",
                    "name", "Acct-" + i,
                    "accountType", (i % 2 == 0) ? "MERCHANT" : "PERSONAL",
                    "txId", "TXN-900" + i
            );
            if (previous != null) {
                previous.addEdge("TRANSFER", current, T.id, 1000L + i, "amount", String.valueOf(10 * i));
            }
            previous = current;
        }

        return new GremlinExecutionService(provider);
    }

    @Test
    void executesTenHopRepeatOutQuery() throws ScriptException {
        GremlinExecutionService service = newSeededTenHopService();

        GremlinExecutionResult result = service.execute(
                "g.V(1).hasLabel('Account').has('txId','TXN-9001').repeat(out('TRANSFER')).times(10).values('name')"
        );

        assertEquals(1, result.resultCount());
        assertEquals("Acct-11", result.results().get(0));
    }

    @Test
    void executesPathQueryWithSimplePath() throws ScriptException {
        GremlinExecutionService service = newSeededTenHopService();

        GremlinExecutionResult result = service.execute(
                "g.V(1).repeat(out().simplePath()).times(10).path().limit(100)"
        );

        assertEquals(1, result.resultCount());
        assertInstanceOf(List.class, result.results().get(0));
    }

    @Test
    void executesQueryThroughTransactionApi() throws ScriptException {
        GremlinExecutionService service = newSeededTenHopService();

        GremlinTransactionalExecutionResult result = service.executeInTransaction(
                "g.V(1).hasLabel('Account').repeat(out('TRANSFER')).times(10).values('name')"
        );

        assertEquals(1, result.resultCount());
        assertEquals("Acct-11", result.results().get(0));
        assertEquals("EXECUTED", result.transactionStatus());
    }

    /**
     * Reproduces C1: project + outE count + order().by(select(),Order.desc) on
     * an AML-shaped mini graph. Verifies that Order.desc is in scope and that
     * results are returned in descending outDegree order.
     */
    @Test
    void c1_projectOutDegreeOrderBySelectDesc() throws Exception {
        TinkerGraphProvider provider = new TinkerGraphProvider();
        TinkerGraph providerGraph = provider.getGraph();

        // Safe collect-then-remove (avoids ConcurrentModificationException)
        List<org.apache.tinkerpop.gremlin.structure.Edge> edges = new ArrayList<>();
        providerGraph.edges().forEachRemaining(edges::add);
        edges.forEach(Element::remove);
        List<Vertex> vertices = new ArrayList<>();
        providerGraph.vertices().forEachRemaining(vertices::add);
        vertices.forEach(Element::remove);

        // Seed mini AML graph: 3 accounts, TRANSFER edges
        // ACC-1 → ACC-2 (×3), ACC-1 → ACC-3 (×1), ACC-2 → ACC-3 (×1)
        Vertex pa1 = providerGraph.addVertex(T.id, 101L, T.label, "Account",
                "accountId", "ACC-1", "bankId", "BANK-A");
        Vertex pa2 = providerGraph.addVertex(T.id, 102L, T.label, "Account",
                "accountId", "ACC-2", "bankId", "BANK-A");
        Vertex pa3 = providerGraph.addVertex(T.id, 103L, T.label, "Account",
                "accountId", "ACC-3", "bankId", "BANK-B");
        pa1.addEdge("TRANSFER", pa2, T.id, 200L, "amount", "100");
        pa1.addEdge("TRANSFER", pa2, T.id, 201L, "amount", "200");
        pa1.addEdge("TRANSFER", pa2, T.id, 202L, "amount", "300");
        pa1.addEdge("TRANSFER", pa3, T.id, 203L, "amount", "50");
        pa2.addEdge("TRANSFER", pa3, T.id, 204L, "amount", "75");

        GremlinExecutionService service = new GremlinExecutionService(provider);

        GremlinExecutionResult result = service.execute(
                "g.V().hasLabel('Account')" +
                ".project('accountId','bankId','outDegree')" +
                ".by('accountId').by('bankId').by(outE('TRANSFER').count())" +
                ".order().by(select('outDegree'),Order.desc).limit(15)"
        );

        // Should return 3 rows (all Account vertices), not 0
        assertEquals(3, result.resultCount(),
                "C1 project+order query must return all 3 Account vertices, not 0");

        // First row must be ACC-1 (4 out-edges), second ACC-2 (1), third ACC-3 (0)
        @SuppressWarnings("unchecked")
        Map<String,Object> first = (Map<String,Object>) result.results().get(0);
        assertEquals("ACC-1", first.get("accountId"), "ACC-1 has most out-transfers → first");

        @SuppressWarnings("unchecked")
        Map<String,Object> second = (Map<String,Object>) result.results().get(1);
        assertEquals("ACC-2", second.get("accountId"), "ACC-2 has 1 out-transfer → second");

        @SuppressWarnings("unchecked")
        Map<String,Object> third = (Map<String,Object>) result.results().get(2);
        assertEquals("ACC-3", third.get("accountId"), "ACC-3 has 0 out-transfers → third");
    }
}
