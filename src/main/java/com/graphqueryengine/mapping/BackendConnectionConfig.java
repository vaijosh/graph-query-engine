package com.graphqueryengine.mapping;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Connection details for a named backend datasource declared inside a mapping file.
 *
 * <p>When a mapping is uploaded the engine automatically registers any backends listed
 * in the mapping's {@code backends} section — no separate {@code POST /backends/register}
 * call is needed.
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
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BackendConnectionConfig(
        @JsonInclude(JsonInclude.Include.NON_NULL) String url,
        @JsonInclude(JsonInclude.Include.NON_NULL) String user,
        @JsonInclude(JsonInclude.Include.NON_NULL) String password,
        @JsonInclude(JsonInclude.Include.NON_NULL) String driverClass
) {
    public BackendConnectionConfig {
        url        = url        != null ? url.trim()        : null;
        // Default user to "sa" for H2 connections when no user is specified,
        // since an empty-string username causes H2 authentication to fail.
        if (user == null || user.isBlank()) {
            user = (url != null && url.startsWith("jdbc:h2:")) ? "sa" : "";
        } else {
            user = user.trim();
        }
        password   = password   != null ? password.trim()   : "";
        driverClass = driverClass != null ? driverClass.trim() : null;
    }
}

