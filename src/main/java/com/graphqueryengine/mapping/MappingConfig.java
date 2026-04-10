package com.graphqueryengine.mapping;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Collections;
import java.util.Map;

/**
 * Top-level mapping configuration that wires Gremlin vertex/edge labels to SQL tables.
 *
 * <h3>backends (optional)</h3>
 * <p>Connection details for each named datasource referenced by this mapping.
 * When a mapping is uploaded the engine automatically registers any backends declared
 * here — no separate {@code POST /backends/register} call is needed.
 *
 * <pre>{@code
 * {
 *   "backends": {
 *     "iceberg": {
 *       "url":         "jdbc:trino://localhost:8080/iceberg",
 *       "user":        "admin",
 *       "driverClass": "io.trino.jdbc.TrinoDriver"
 *     }
 *   },
 *   "defaultDatasource": "iceberg",
 *   "vertices": { ... },
 *   "edges":    { ... }
 * }
 * }</pre>
 *
 * <h3>defaultDatasource</h3>
 * <p>Optional — backend id applied to any vertex or edge that does not declare its own
 * {@code datasource} field.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MappingConfig(
        @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, BackendConnectionConfig> backends,
        @JsonInclude(JsonInclude.Include.NON_NULL)  String defaultDatasource,
        Map<String, VertexMapping> vertices,
        Map<String, EdgeMapping> edges
) {
    /** Backward-compatible constructor for existing code/tests that don't supply backends or defaultDatasource. */
    public MappingConfig(Map<String, VertexMapping> vertices, Map<String, EdgeMapping> edges) {
        this(null, null, vertices, edges);
    }

    public MappingConfig {
        backends          = backends == null ? Collections.emptyMap() : backends;
        defaultDatasource = (defaultDatasource == null || defaultDatasource.isBlank())
                ? null : defaultDatasource.trim();
        vertices = vertices == null ? Collections.emptyMap() : vertices;
        edges    = edges    == null ? Collections.emptyMap() : edges;
    }

    /**
     * Resolves the effective datasource for a given vertex label.
     * Prefers the vertex-level declaration; falls back to {@link #defaultDatasource()}.
     */
    public String datasourceForVertex(String label) {
        VertexMapping vm = vertices.get(label);
        if (vm != null && vm.datasource() != null) return vm.datasource();
        return defaultDatasource;
    }

    /**
     * Resolves the effective datasource for a given edge label.
     * Prefers the edge-level declaration; falls back to {@link #defaultDatasource()}.
     */
    public String datasourceForEdge(String label) {
        EdgeMapping em = edges.get(label);
        if (em != null && em.datasource() != null) return em.datasource();
        return defaultDatasource;
    }
}
