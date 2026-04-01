package com.graphqueryengine.mapping;

import java.util.Collections;
import java.util.Map;

public record MappingConfig(
        Map<String, VertexMapping> vertices,
        Map<String, EdgeMapping> edges
) {
    public MappingConfig {
        vertices = vertices == null ? Collections.emptyMap() : vertices;
        edges = edges == null ? Collections.emptyMap() : edges;
    }
}

