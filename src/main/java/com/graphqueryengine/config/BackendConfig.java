package com.graphqueryengine.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

/**
 * Configuration for one named SQL backend (JDBC data source).
 *
 * <p>Multiple backends are configured via the {@code BACKENDS} environment variable as a
 * JSON array:
 * <pre>{@code
 * BACKENDS='[
 *   {"id":"h2",      "url":"jdbc:h2:file:./data/graph;AUTO_SERVER=TRUE","user":"sa","password":""},
 *   {"id":"iceberg", "url":"jdbc:trino://localhost:8080/iceberg/aml",   "user":"admin","password":""}
 * ]'
 * }</pre>
 *
 * <p>The {@code driverClass} field is optional — if absent the driver is inferred from the
 * URL prefix via {@link DatabaseConfig#inferDriverClass(String)}.
 *
 * <p>When {@code BACKENDS} is not set the engine falls back to the legacy
 * {@code DB_URL} / {@code DB_USER} / {@code DB_PASSWORD} / {@code DB_DRIVER} env vars,
 * constructing a single backend with id {@value #LEGACY_ID}.
 */
public record BackendConfig(String id, String url, String user, String password, String driverClass) {

    /** Backend id used when created from legacy {@code DB_URL} env var. */
    public static final String LEGACY_ID = "default";

    /**
     * Parses the list of backend configs from the environment.
     *
     * <p>Uses {@code BACKENDS} JSON array when present; falls back to the legacy single-backend
     * env vars otherwise.
     */
    public static List<BackendConfig> listFromEnvironment() {
        String raw = System.getenv("BACKENDS");
        if (raw != null && !raw.isBlank()) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                List<BackendConfigJson> parsed = mapper.readValue(
                        raw.trim(), new TypeReference<>() {});
                if (parsed.isEmpty()) {
                    throw new IllegalArgumentException("BACKENDS array must not be empty");
                }
                return parsed.stream()
                        .map(b -> {
                            String driver = (b.driverClass() != null && !b.driverClass().isBlank())
                                    ? b.driverClass()
                                    : DatabaseConfig.inferDriverClass(b.url());
                            return new BackendConfig(
                                    b.id()       != null ? b.id()       : LEGACY_ID,
                                    b.url()      != null ? b.url()      : "",
                                    b.user()     != null ? b.user()     : "sa",
                                    b.password() != null ? b.password() : "",
                                    driver
                            );
                        })
                        .toList();
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Failed to parse BACKENDS env var as JSON array: " + e.getMessage(), e);
            }
        }

        // Legacy fallback: single backend from DB_URL etc.
        DatabaseConfig legacy = DatabaseConfig.fromEnvironment();
        return List.of(new BackendConfig(
                LEGACY_ID,
                legacy.url(),
                legacy.user(),
                legacy.password(),
                legacy.driverClass()
        ));
    }

    // -------------------------------------------------------------------------
    // Internal DTO for Jackson deserialization (allows partial fields)
    // -------------------------------------------------------------------------

    private record BackendConfigJson(
            String id,
            String url,
            String user,
            String password,
            String driverClass) {

        // Jackson needs a no-arg constructor or @JsonCreator; we use a custom deserializer
        // workaround: Jackson can deserialize records if compiled with Java 16+.
        // For wider compatibility we keep a plain record and rely on Jackson's record support.
    }
}

