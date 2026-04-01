package com.graphqueryengine.gremlin;

import java.util.List;

public record GremlinExecutionResult(String gremlin, List<Object> results, int resultCount) {
}

