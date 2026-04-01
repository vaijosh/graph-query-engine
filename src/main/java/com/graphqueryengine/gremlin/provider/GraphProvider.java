package com.graphqueryengine.gremlin.provider;

import com.graphqueryengine.gremlin.GremlinExecutionResult;
import com.graphqueryengine.gremlin.GremlinTransactionalExecutionResult;

import javax.script.ScriptException;

public interface GraphProvider extends AutoCloseable {
    String providerId();

    GremlinExecutionResult execute(String gremlin) throws ScriptException;

    GremlinTransactionalExecutionResult executeInTransaction(String gremlin) throws ScriptException;

    void resetTransactionDemoGraph();


    @Override
    default void close() {
        // Default no-op.
    }
}

