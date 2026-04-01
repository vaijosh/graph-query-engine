package com.graphqueryengine.gremlin;

import java.util.List;

public record GremlinTransactionalExecutionResult(
        String gremlin,
        List<Object> results,
        int resultCount,
        String transactionMode,
        String transactionStatus
) {
}

