package com.graphqueryengine.gremlin.provider;

import com.graphqueryengine.gremlin.GremlinExecutionResult;
import com.graphqueryengine.gremlin.GremlinTransactionalExecutionResult;

import javax.script.ScriptException;

public interface GraphProvider extends AutoCloseable {
    String providerId();

    GremlinExecutionResult execute(String gremlin) throws ScriptException;

    GremlinTransactionalExecutionResult executeInTransaction(String gremlin) throws ScriptException;

    /**
     * Resets the in-memory graph to a clean empty state so transaction-demo
     * scenarios can be replayed from scratch.  Providers that do not support
     * an in-memory graph may leave this as a no-op.
     */
    default void resetTransactionDemoGraph() {
        // Default no-op for providers that don't need reset.
    }

    @Override
    default void close() {
        // Default no-op.
    }
}

