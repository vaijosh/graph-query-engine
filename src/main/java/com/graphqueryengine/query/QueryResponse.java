package com.graphqueryengine.query;

import java.util.List;
import java.util.Map;

public record QueryResponse(String sql, List<Object> parameters, List<Map<String, Object>> rows, int rowCount) {
}

