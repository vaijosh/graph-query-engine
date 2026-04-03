package com.graphqueryengine.mapping;

import com.fasterxml.jackson.annotation.JsonAlias;
import java.util.Collections;
import java.util.Map;

public record EdgeMapping(
        String table,
        String idColumn,
        String outColumn,
        String inColumn,
        Map<String, String> properties,
        @JsonAlias({"sourceLabel", "outLabel"}) String outVertexLabel,
        @JsonAlias({"targetLabel", "inLabel"}) String inVertexLabel
) {
    // Backward-compatible ctor for existing code/tests and legacy mapping JSON.
    public EdgeMapping(String table,
                       String idColumn,
                       String outColumn,
                       String inColumn,
                       Map<String, String> properties) {
        this(table, idColumn, outColumn, inColumn, properties, null, null);
    }

    public EdgeMapping {
        properties = properties == null ? Collections.emptyMap() : properties;
        outVertexLabel = normalizeLabel(outVertexLabel);
        inVertexLabel = normalizeLabel(inVertexLabel);
    }

    @Override
    public String table() {
        return TableReferenceResolver.toSqlReference(table);
    }

    private static String normalizeLabel(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }
}
