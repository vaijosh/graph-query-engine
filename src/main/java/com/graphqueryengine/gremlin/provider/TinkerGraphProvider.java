package com.graphqueryengine.gremlin.provider;

import com.graphqueryengine.gremlin.GremlinExecutionResult;
import com.graphqueryengine.gremlin.GremlinTransactionalExecutionResult;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class TinkerGraphProvider implements GraphProvider {
    private static final Logger LOG = Logger.getLogger(TinkerGraphProvider.class.getName());

    /** Default path for the persisted graph file (Gryo binary format). */
    private static final String DEFAULT_GRAPH_LOCATION = "./data/graph.gryo";

    private final GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine();
    private final String graphLocation;

    private TinkerGraph graph;
    private GraphTraversalSource traversal;
    private GraphTransactionApi transactionApi;

    public TinkerGraphProvider() {
        this(resolveGraphLocation());
    }

    public TinkerGraphProvider(String graphLocation) {
        this.graphLocation = graphLocation;
        initializeIfNeeded();
    }

    private static String resolveGraphLocation() {
        String env = System.getenv("TINKERGRAPH_LOCATION");
        return (env != null && !env.isBlank()) ? env : DEFAULT_GRAPH_LOCATION;
    }

    @Override
    public String providerId() {
        return "tinkergraph";
    }

    @Override
    public synchronized GremlinExecutionResult execute(String gremlin) throws ScriptException {
        if (gremlin == null || gremlin.isBlank()) {
            throw new IllegalArgumentException("Gremlin query is required");
        }

        initializeIfNeeded();

        Bindings bindings = new SimpleBindings();
        bindings.put("g", traversal);

        Object raw = scriptEngine.eval(gremlin, bindings);
        List<Object> normalized = normalizeResult(raw);
        return new GremlinExecutionResult(gremlin, normalized, normalized.size());
    }

    @Override
    public synchronized GremlinTransactionalExecutionResult executeInTransaction(String gremlin) throws ScriptException {
        initializeIfNeeded();
        try {
            GraphTransactionApi.TransactionExecution<GremlinExecutionResult> txExecution = transactionApi.execute(() -> execute(gremlin));
            GremlinExecutionResult result = txExecution.value();
            persistGraph();
            return new GremlinTransactionalExecutionResult(
                    result.gremlin(),
                    result.results(),
                    result.resultCount(),
                    txExecution.mode(),
                    txExecution.status()
            );
        } catch (ScriptException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException("Transactional Gremlin execution failed", ex);
        }
    }


    /**
     * Exposes the underlying TinkerGraph for demo/seeding utilities in the {@code demo} module.
     * Not intended for use by the core engine.
     */
    public synchronized TinkerGraph getGraph() {
        initializeIfNeeded();
        return graph;
    }

    /**
     * Exposes the traversal source for demo/seeding utilities in the {@code demo} module.
     * Not intended for use by the core engine.
     */
    public synchronized GraphTraversalSource getTraversal() {
        initializeIfNeeded();
        return traversal;
    }

    @SuppressWarnings("DataFlowIssue")
    private void initializeIfNeeded() {
        if (graph != null && traversal != null && transactionApi != null) {
            return;
        }

        graph = TinkerGraph.open();

        File graphFile = new File(graphLocation);
        if (graphFile.exists()) {
            try {
                graph.io(GryoIo.build()).readGraph(graphLocation);
                long vertexCount = graph.traversal().V().count().next();
                long edgeCount = graph.traversal().E().count().next();
                LOG.info("Loaded persistent graph from " + graphLocation
                        + " (" + vertexCount + " vertices, " + edgeCount + " edges)");
            } catch (Exception e) {
                LOG.warning("Failed to load graph from " + graphLocation + ": " + e.getMessage()
                        + " — starting with empty graph.");
                graph.close();
                graph = TinkerGraph.open();
            }
        } else {
            LOG.info("No existing graph file at " + graphLocation + " — starting with empty in-memory graph.");
        }

        traversal = graph.traversal();
        transactionApi = new TinkerGraphTransactionApi(graph);
    }

    /** Write the current graph state to the configured file location. */
    @SuppressWarnings("DataFlowIssue")
    private void persistGraph() {
        if (graphLocation == null || graphLocation.isBlank()) {
            return;
        }
        try {
            File parent = new File(graphLocation).getParentFile();
            if (parent != null && !parent.exists()) {
                boolean ignored = parent.mkdirs();
            }
            graph.io(GryoIo.build()).writeGraph(graphLocation);
        } catch (Exception e) {
            LOG.warning("Failed to persist graph to " + graphLocation + ": " + e.getMessage());
        }
    }

    private List<Object> normalizeResult(Object raw) {
        List<Object> results = new ArrayList<>();
        if (raw == null) {
            return results;
        }

        if (raw instanceof Traversal<?, ?> traversalResult) {
            while (traversalResult.hasNext()) {
                results.add(normalizeValue(traversalResult.next()));
            }
            return results;
        }

        if (raw instanceof Iterable<?> iterable) {
            for (Object item : iterable) {
                results.add(normalizeValue(item));
            }
            return results;
        }

        results.add(normalizeValue(raw));
        return results;
    }

    private Object normalizeValue(Object value) {
        if (value instanceof Vertex vertex) {
            return vertexToMap(vertex);
        }
        if (value instanceof Edge edge) {
            return edgeToMap(edge);
        }
        if (value instanceof Path path) {
            List<Object> nodes = new ArrayList<>();
            for (Object step : path.objects()) {
                nodes.add(normalizeValue(step));
            }
            return nodes;
        }
        if (value instanceof List<?> list) {
            List<Object> normalized = new ArrayList<>();
            for (Object item : list) {
                normalized.add(normalizeValue(item));
            }
            return normalized;
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                normalized.put(String.valueOf(entry.getKey()), normalizeValue(entry.getValue()));
            }
            return normalized;
        }
        return value;
    }

    private Map<String, Object> vertexToMap(Vertex vertex) {
        Map<String, Object> properties = new LinkedHashMap<>();
        for (String key : vertex.keys()) {
            properties.put(key, vertex.property(key).value());
        }
        Map<String, Object> vertexMap = new LinkedHashMap<>();
        vertexMap.put("id", vertex.id());
        vertexMap.put("label", vertex.label());
        vertexMap.put("properties", properties);
        return vertexMap;
    }

    private Map<String, Object> edgeToMap(Edge edge) {
        Map<String, Object> properties = new LinkedHashMap<>();
        for (String key : edge.keys()) {
            properties.put(key, edge.property(key).value());
        }
        Map<String, Object> edgeMap = new LinkedHashMap<>();
        edgeMap.put("id", edge.id());
        edgeMap.put("label", edge.label());
        edgeMap.put("outV", edge.outVertex().id());
        edgeMap.put("inV", edge.inVertex().id());
        edgeMap.put("properties", properties);
        return edgeMap;
    }

    @Override
    public synchronized void close() {
        if (traversal != null) {
            try {
                traversal.close();
            } catch (Exception ex) {
                throw new IllegalStateException("Failed to close traversal source", ex);
            }
            traversal = null;
        }
        if (graph != null) {
            graph.close();
            graph = null;
        }
        transactionApi = null;
    }
}
