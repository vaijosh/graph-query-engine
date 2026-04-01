package com.graphqueryengine.mapping;

import java.util.Collections;
import java.util.Map;

public record VertexMapping(
        String table,
        String idColumn,
        Map<String, String> properties
) {
    public VertexMapping {
        properties = properties == null ? Collections.emptyMap() : properties;
    }
}

