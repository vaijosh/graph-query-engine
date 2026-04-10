package com.graphqueryengine.mapping;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Collections;
import java.util.Map;

/**
 * Maps a Gremlin edge label to a SQL table.
 * Unknown JSON fields are silently ignored.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EdgeMapping(
        String table,
        String idColumn,
        String outColumn,
        String inColumn,
        Map<String, String> properties,
        @JsonAlias({"sourceLabel", "outLabel"}) String outVertexLabel,
        @JsonAlias({"targetLabel", "inLabel"}) String inVertexLabel,
        @JsonInclude(JsonInclude.Include.NON_NULL) String datasource
) {
    // Backward-compatible ctors for existing code/tests and legacy mapping JSON.
    public EdgeMapping(String table,
                       String idColumn,
                       String outColumn,
                       String inColumn,
                       Map<String, String> properties) {
        this(table, idColumn, outColumn, inColumn, properties, null, null, null);
    }

    public EdgeMapping(String table,
                       String idColumn,
                       String outColumn,
                       String inColumn,
                       Map<String, String> properties,
                       String outVertexLabel,
                       String inVertexLabel) {
        this(table, idColumn, outColumn, inColumn, properties, outVertexLabel, inVertexLabel, null);
    }

    public EdgeMapping {
        properties     = properties == null ? Collections.emptyMap() : properties;
        outVertexLabel = normalizeLabel(outVertexLabel);
        inVertexLabel  = normalizeLabel(inVertexLabel);
        datasource     = (datasource == null || datasource.isBlank()) ? null : datasource.trim();
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
