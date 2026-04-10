package com.graphqueryengine.mapping;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Collections;
import java.util.Map;

/**
 * Maps a Gremlin vertex label to a SQL table.
 * Unknown JSON fields are silently ignored.
 * // ...existing code...
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record VertexMapping(
        String table,
        String idColumn,
        @JsonInclude(JsonInclude.Include.NON_NULL) String datasource,
        Map<String, String> properties
) {
    /** Backward-compatible constructor for code/tests that don't supply a datasource. */
    public VertexMapping(String table, String idColumn, Map<String, String> properties) {
        this(table, idColumn, null, properties);
    }

    public VertexMapping {
        properties = properties == null ? Collections.emptyMap() : properties;
        datasource = (datasource == null || datasource.isBlank()) ? null : datasource.trim();
    }

    @Override
    public String table() {
        return TableReferenceResolver.toSqlReference(table);
    }

}
