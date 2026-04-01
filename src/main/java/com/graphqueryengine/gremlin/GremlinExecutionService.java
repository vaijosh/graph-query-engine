package com.graphqueryengine.gremlin;

import com.graphqueryengine.gremlin.provider.GraphProvider;
import com.graphqueryengine.gremlin.provider.GraphProviderFactory;

import javax.script.ScriptException;

public class GremlinExecutionService {
    private final GraphProvider graphProvider;

    public GremlinExecutionService() {
        this(GraphProviderFactory.fromEnvironment());
    }

    public GremlinExecutionService(GraphProvider graphProvider) {
        this.graphProvider = graphProvider;
    }

    public synchronized GremlinExecutionResult execute(String gremlin) throws ScriptException {
        return graphProvider.execute(gremlin);
    }

    public synchronized GremlinTransactionalExecutionResult executeInTransaction(String gremlin) throws ScriptException {
        return graphProvider.executeInTransaction(gremlin);
    }

    public String providerId() {
        return graphProvider.providerId();
    }

    /**
     * Returns the underlying {@link GraphProvider} so demo/admin code can cast it
     * to a concrete type (e.g. {@link com.graphqueryengine.gremlin.provider.TinkerGraphProvider})
     * without leaking demo concerns into the service API.
     */
    public GraphProvider provider() {
        return graphProvider;
    }
}

