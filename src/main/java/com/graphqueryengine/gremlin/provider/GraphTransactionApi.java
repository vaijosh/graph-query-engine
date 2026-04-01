package com.graphqueryengine.gremlin.provider;

public interface GraphTransactionApi {

    <T> TransactionExecution<T> execute(TransactionWork<T> work) throws Exception;

    record TransactionExecution<T>(T value, String mode, String status) {
    }

    @FunctionalInterface
    interface TransactionWork<T> {
        T run() throws Exception;
    }
}

