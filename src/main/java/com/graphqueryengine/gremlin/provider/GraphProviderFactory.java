package com.graphqueryengine.gremlin.provider;

import com.graphqueryengine.config.BackendConfig;
import com.graphqueryengine.config.DatabaseConfig;
import com.graphqueryengine.db.DatabaseManager;
import com.graphqueryengine.mapping.MappingStore;

/**
 * Factory for creating {@link GraphProvider} instances.
 *
 * <p>Only one provider type exists: {@code "sql"} (the default).
 * WCOJ acceleration is an <em>opt-out</em> feature of {@link SqlGraphProvider}
 * controlled by the {@code WCOJ_ENABLED} environment variable (default: {@code true}).
 *
 * <h3>Environment variables</h3>
 * <pre>
 *   GRAPH_PROVIDER=sql         (only valid value; "sql" is the default)
 *   WCOJ_ENABLED=true|false    (default true  — enable/disable leapfrog optimiser)
 *   WCOJ_MAX_EDGES=&lt;n&gt;         (default 5_000_000 — in-memory edge threshold)
 *   WCOJ_DISK_QUOTA_MB=&lt;n&gt;    (default 2048 — per-mapping disk quota in MB)
 * </pre>
 */
public final class GraphProviderFactory {

    private GraphProviderFactory() {
    }

    public static GraphProvider fromEnvironment() {
        return fromEnvironment(null, null);
    }

    public static GraphProvider fromEnvironment(DatabaseManager databaseManager, MappingStore mappingStore) {
        // GRAPH_PROVIDER is kept for forward-compatibility, but only "sql" is supported.
        String configuredProvider = System.getenv()
                .getOrDefault("GRAPH_PROVIDER", ProviderConstants.PROVIDER_SQL).trim().toLowerCase();
        return fromProviderName(configuredProvider, databaseManager, mappingStore);
    }

    public static GraphProvider fromProviderName(String configuredProvider,
                                                  DatabaseManager databaseManager,
                                                  MappingStore mappingStore) {
        if (ProviderConstants.PROVIDER_SQL.equals(configuredProvider)) {
            if (databaseManager == null || mappingStore == null) {
                throw new IllegalStateException(
                        "GRAPH_PROVIDER=sql requires a DatabaseManager and MappingStore. " +
                        "Ensure DB_URL, DB_USER, and DB_DRIVER are configured.");
            }
            return new SqlGraphProvider(databaseManager, mappingStore);
        }

        throw new IllegalArgumentException("Unsupported GRAPH_PROVIDER: '" + configuredProvider +
                "'. Only 'sql' is supported. WCOJ is enabled via WCOJ_ENABLED=true (the default).");
    }

    /**
     * Creates a {@link SqlGraphProvider} directly from a {@link BackendConfig}.
     * Used by {@link BackendRegistry} to avoid duplicating driver-loading logic.
     */
    public static SqlGraphProvider fromBackendConfig(BackendConfig cfg, MappingStore mappingStore) {
        DatabaseConfig dbConfig = new DatabaseConfig(
                cfg.url(), cfg.user(), cfg.password(), cfg.driverClass());
        DatabaseManager dm = new DatabaseManager(dbConfig);
        return new SqlGraphProvider(dm, mappingStore);
    }
}
