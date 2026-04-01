package com.graphqueryengine.gremlin.provider;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;

public class TinkerGraphTransactionApi implements GraphTransactionApi {
    private final Graph graph;

    public TinkerGraphTransactionApi(Graph graph) {
        this.graph = graph;
    }

    @Override
    public <T> TransactionExecution<T> execute(TransactionWork<T> work) throws Exception {
        if (!graph.features().graph().supportsTransactions()) {
            T value = work.run();
            return new TransactionExecution<>(value, "NON_TRANSACTIONAL_GRAPH", "EXECUTED");
        }

        Transaction tx = graph.tx();
        tx.open();
        try {
            T value = work.run();
            tx.commit();
            return new TransactionExecution<>(value, "NATIVE_GRAPH_TX", "COMMITTED");
        } catch (Exception ex) {
            tx.rollback();
            throw ex;
        }
    }
}

