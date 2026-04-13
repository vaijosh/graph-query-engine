package com.graphqueryengine.gremlin.provider;

import com.graphqueryengine.config.BackendConfig;
import com.graphqueryengine.config.DatabaseConfig;
import com.graphqueryengine.db.DatabaseManager;
import com.graphqueryengine.engine.wcoj.AdjacencyIndexRegistry;
import com.graphqueryengine.engine.wcoj.WcojGraphProvider;
import com.graphqueryengine.mapping.MappingStore;

public final class GraphProviderFactory {
    private GraphProviderFactory() {
    }

    public static GraphProvider fromEnvironment() {
        return fromEnvironment(null, null);
    }

    public static GraphProvider fromEnvironment(DatabaseManager databaseManager, MappingStore mappingStore) {
        String configuredProvider = System.getenv()
                .getOrDefault("GRAPH_PROVIDER", "sql").trim().toLowerCase();
        return fromProviderName(configuredProvider, databaseManager, mappingStore);
    }

    public static GraphProvider fromProviderName(String configuredProvider,
                                                  DatabaseManager databaseManager,
                                                  MappingStore mappingStore) {

        if ("sql".equals(configuredProvider)) {
            if (databaseManager == null || mappingStore == null) {
                throw new IllegalStateException(
                        "GRAPH_PROVIDER=sql requires a DatabaseManager and MappingStore. " +
                        "Ensure DB_URL, DB_USER, and DB_DRIVER are configured.");
            }
            return new SqlGraphProvider(databaseManager, mappingStore);
        }

        if ("wcoj".equals(configuredProvider)) {
            if (databaseManager == null || mappingStore == null) {
                throw new IllegalStateException(
                        "GRAPH_PROVIDER=wcoj requires a DatabaseManager and MappingStore. " +
                        "Ensure DB_URL, DB_USER, and DB_DRIVER are configured.");
            }
            long maxEdgesInMemory = AdjacencyIndexRegistry.DEFAULT_MAX_EDGES_IN_MEMORY;
            String maxEdgesEnv = System.getenv("WCOJ_MAX_EDGES");
            if (maxEdgesEnv != null && !maxEdgesEnv.isBlank()) {
                try { maxEdgesInMemory = Long.parseLong(maxEdgesEnv.trim()); }
                catch (NumberFormatException ignored) { /* keep default */ }
            }

            long diskQuotaMb = 2048L; // 2 GB default
            String diskQuotaEnv = System.getenv("WCOJ_DISK_QUOTA_MB");
            if (diskQuotaEnv != null && !diskQuotaEnv.isBlank()) {
                try { diskQuotaMb = Long.parseLong(diskQuotaEnv.trim()); }
                catch (NumberFormatException ignored) { /* keep default */ }
            }

            return new WcojGraphProvider(databaseManager, mappingStore,
                    new AdjacencyIndexRegistry(
                            maxEdgesInMemory,
                            diskQuotaMb * 1024 * 1024,
                            AdjacencyIndexRegistry.DEFAULT_CACHE_DIR));
        }

        throw new IllegalArgumentException("Unsupported GRAPH_PROVIDER: " + configuredProvider +
                ". Supported providers: sql, wcoj");
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

