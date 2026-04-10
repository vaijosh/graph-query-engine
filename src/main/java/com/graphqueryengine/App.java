package com.graphqueryengine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.graphqueryengine.config.DatabaseConfig;
import com.graphqueryengine.db.DatabaseManager;
import com.graphqueryengine.gremlin.GremlinExecutionResult;
import com.graphqueryengine.gremlin.GremlinExecutionService;
import com.graphqueryengine.gremlin.GremlinTransactionalExecutionResult;
import com.graphqueryengine.gremlin.provider.BackendRegistry;
import com.graphqueryengine.gremlin.provider.GraphProviderFactory;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.MappingStore;
import com.graphqueryengine.query.api.GraphQueryTranslator;
import com.graphqueryengine.query.api.QueryExplanation;
import com.graphqueryengine.query.api.QueryRequest;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.factory.DefaultGraphQueryTranslatorFactory;
import com.graphqueryengine.query.factory.GraphQueryTranslatorFactory;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.UploadedFile;
import io.javalin.json.JavalinJackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.sql.Connection;
import java.util.*;
import java.io.IOException;
import javax.script.ScriptException;

public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);
    private static final String SQL_TRACE_HEADER   = "X-SQL-Trace";
    private static final String MAPPING_ID_HEADER  = "X-Mapping-Id";
    private static final String BACKEND_ID_HEADER  = "X-Backend-Id";
    private static final String ACTIVE_MAPPING_FILE = ".active-mapping";

    public static void main(String[] args) {
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "7000"));
        start(port);
    }

    public static Javalin start(int port) {
        return start(port, (app, svc) -> {}, new DefaultGraphQueryTranslatorFactory());
    }

    /**
     * Test-friendly overload: starts the engine using an explicit provider name,
     * bypassing the {@code GRAPH_PROVIDER} environment variable.
     * Use {@code "tinkergraph"} in tests that don't need a database.
     */
    public static Javalin start(int port, String providerName) {
        return start(port, providerName, (app, svc) -> {}, new DefaultGraphQueryTranslatorFactory());
    }

    public static Javalin start(int port,
                                String providerName,
                                java.util.function.BiConsumer<Javalin, GremlinExecutionService> extensions,
                                GraphQueryTranslatorFactory translatorFactory) {
        // Explicit provider name (e.g. "tinkergraph" in tests) — bypass BackendRegistry.
        MappingStore mappingStore = new MappingStore();
        GraphQueryTranslator translator = translatorFactory.create();
        if ("tinkergraph".equals(providerName)) {
            GremlinExecutionService gremlinExecutionService = new GremlinExecutionService(
                    GraphProviderFactory.fromProviderName(providerName, null, mappingStore));
            return buildAndStartApp(port, gremlinExecutionService, null, translator, extensions, mappingStore,
                    new DatabaseConfig("(tinkergraph)", "", "", ""));
        }
        DatabaseConfig databaseConfig = DatabaseConfig.fromEnvironment();
        DatabaseManager databaseManager = new DatabaseManager(databaseConfig);
        ensureDatabaseFileInitialized(databaseManager, databaseConfig);
        GremlinExecutionService gremlinExecutionService = new GremlinExecutionService(
                GraphProviderFactory.fromProviderName(providerName, databaseManager, mappingStore));
        return buildAndStartApp(port, gremlinExecutionService, null, translator, extensions, mappingStore, databaseConfig);
    }

    /**
     * Starts the engine and invokes {@code extensions} so callers (e.g. the demo module)
     * can register additional routes against the same {@link Javalin} instance without
     * creating a dependency from the engine onto demo code.
     */
    public static Javalin start(int port, java.util.function.BiConsumer<Javalin, GremlinExecutionService> extensions) {
        return start(port, extensions, new DefaultGraphQueryTranslatorFactory());
    }

    public static Javalin start(int port,
                                java.util.function.BiConsumer<Javalin, GremlinExecutionService> extensions,
                                GraphQueryTranslatorFactory translatorFactory) {
        MappingStore mappingStore = new MappingStore();
        GraphQueryTranslator translator = translatorFactory.create();

        String configuredProvider = System.getenv().getOrDefault("GRAPH_PROVIDER", "sql").trim().toLowerCase();
        if ("tinkergraph".equals(configuredProvider)) {
            GremlinExecutionService gremlinExecutionService = new GremlinExecutionService(
                    GraphProviderFactory.fromProviderName("tinkergraph", null, mappingStore));
            return buildAndStartApp(port, gremlinExecutionService, null, translator, extensions, mappingStore,
                    new DatabaseConfig("(tinkergraph)", "", "", ""));
        }

        // WCOJ path: uses its own DatabaseManager + AdjacencyIndexRegistry (disk-backed).
        // Does NOT use BackendRegistry — WCOJ manages its own JDBC connection pool.
        if ("wcoj".equals(configuredProvider)) {
            DatabaseConfig databaseConfig = DatabaseConfig.fromEnvironment();
            DatabaseManager databaseManager = new DatabaseManager(databaseConfig);
            ensureDatabaseFileInitialized(databaseManager, databaseConfig);
            GremlinExecutionService gremlinExecutionService = new GremlinExecutionService(
                    GraphProviderFactory.fromProviderName("wcoj", databaseManager, mappingStore));
            return buildAndStartApp(port, gremlinExecutionService, null, translator, extensions, mappingStore,
                    databaseConfig);
        }

        // SQL path: build multi-backend registry.
        BackendRegistry backendRegistry = BackendRegistry.fromEnvironment(mappingStore);
        // Validate all backends can connect.
        for (BackendRegistry.Entry entry : backendRegistry.listEntries()) {
            LOG.info("Backend verified: id={} url={}", entry.id(), entry.url());
        }
        // Create a registry-aware SqlGraphProvider that routes queries to the correct
        // backend based on the mapping's datasource field. This is the provider used
        // for all requests that don't carry an explicit X-Backend-Id header.
        com.graphqueryengine.gremlin.provider.SqlGraphProvider registryAwareProvider =
                new com.graphqueryengine.gremlin.provider.SqlGraphProvider(backendRegistry, mappingStore);
        GremlinExecutionService registryAwareService = new GremlinExecutionService(registryAwareProvider);
        return buildAndStartApp(port, registryAwareService, backendRegistry, translator, extensions, mappingStore,
                new DatabaseConfig(entry(backendRegistry).url(), "", "", ""));
    }

    /** Returns the first entry of the registry (avoids field access boilerplate). */
    private static BackendRegistry.Entry entry(BackendRegistry r) {
        return r.listEntries().get(0);
    }

    private static Javalin buildAndStartApp(int port,
                                            GremlinExecutionService gremlinExecutionService,
                                            BackendRegistry backendRegistry,
                                            GraphQueryTranslator translator,
                                            java.util.function.BiConsumer<Javalin, GremlinExecutionService> extensions,
                                            MappingStore mappingStore,
                                            DatabaseConfig databaseConfig) {
        ObjectMapper objectMapper = new ObjectMapper();

        Javalin app = Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson());
            config.http.maxRequestSize = 20_000_000L;
        }).start(port);

        Path mappingStoreDir = resolveMappingStoreDir();
        preloadMappingsOnStartup(mappingStore, objectMapper, mappingStoreDir);

        app.get("/health", ctx -> {
            Map<String, Object> health = new LinkedHashMap<>();
            health.put("status", "ok");
            health.put("service", "graph-query-engine");
            health.put("dbUrl", databaseConfig.url());
            health.put("provider", gremlinExecutionService.providerId());
            if (backendRegistry != null) {
                health.put("backends", backendRegistry.listEntries().stream()
                        .map(e -> Map.of("id", e.id(), "url", e.url()))
                        .toList());
                health.put("defaultBackend", backendRegistry.defaultId());
            }
            ctx.json(health);
        });

        app.post("/mapping/upload", ctx -> {
            UploadedFile file = ctx.uploadedFile("file");
            if (file == null) {
                ctx.status(400).json(Map.of("error", "Attach mapping JSON as multipart field named 'file'"));
                return;
            }

            try (InputStream content = file.content()) {
                String json = new String(content.readAllBytes(), StandardCharsets.UTF_8);
                MappingConfig mappingConfig = objectMapper.readValue(json, MappingConfig.class);
                String mappingId = ctx.formParam("id");
                if (mappingId == null || mappingId.isBlank()) {
                    mappingId = ctx.queryParam("id");
                }
                String mappingName = ctx.formParam("name");
                if (mappingName == null || mappingName.isBlank()) {
                    mappingName = ctx.queryParam("name");
                }
                boolean activate = parseEnabledFlag(ctx.queryParam("activate"), true);

                MappingStore.StoredMapping stored = mappingStore.put(mappingId, mappingName, mappingConfig, activate);
                persistMapping(stored, objectMapper, mappingStoreDir);
                if (activate) {
                    persistActiveMappingId(stored.id(), mappingStoreDir);
                }

                // Auto-register any backends declared in the mapping file.
                List<String> autoRegistered = new ArrayList<>();
                if (backendRegistry != null && !mappingConfig.backends().isEmpty()) {
                    mappingConfig.backends().forEach((id, cfg) -> {
                        try {
                            backendRegistry.register(id, cfg.url(), cfg.user(), cfg.password(), mappingStore);
                            autoRegistered.add(id);
                            LOG.info("Auto-registered backend from mapping: id={} url={}", id, cfg.url());
                        } catch (Exception ex) {
                            LOG.warn("Failed to auto-register backend '{}' from mapping: {}", id, ex.getMessage());
                        }
                    });
                }

                Map<String, Object> response = new LinkedHashMap<>();
                response.put("message", "Mapping loaded");
                response.put("mappingId", stored.id());
                response.put("mappingName", stored.name());
                response.put("active", Objects.equals(mappingStore.activeMappingId().orElse(null), stored.id()));
                response.put("vertexLabels", mappingConfig.vertices().keySet());
                response.put("edgeLabels", mappingConfig.edges().keySet());
                response.put("createdAt", stored.createdAt());
                if (!autoRegistered.isEmpty()) {
                    response.put("autoRegisteredBackends", autoRegistered);
                }
                ctx.status(201).json(response);
            } catch (Exception ex) {
                ctx.status(400).json(Map.of("error", ex.getMessage()));
            }
        });

        app.get("/mapping", ctx -> {
            MappingStore.StoredMapping mapping = resolveMappingForRequest(ctx, mappingStore).orElse(null);
            if (mapping == null) {
                ctx.status(404).json(Map.of("error", "No mapping uploaded yet"));
                return;
            }
            ctx.json(Map.of(
                    "mappingId", mapping.id(),
                    "mappingName", mapping.name(),
                    "active", mappingStore.activeMappingId().orElse("").equals(mapping.id()),
                    "createdAt", mapping.createdAt(),
                    "vertices", mapping.config().vertices(),
                    "edges", mapping.config().edges()
            ));
        });

        app.get("/mappings", ctx -> ctx.json(Map.of(
                "activeMappingId", Objects.requireNonNull(mappingStore.activeMappingId().orElse(null)),
                "mappings", mappingStore.list()
        )));

        app.post("/mapping/active", ctx -> {
            String mappingId = ctx.queryParam("id");
            if (mappingId == null || mappingId.isBlank()) {
                ctx.status(400).json(Map.of("error", "Query param 'id' is required"));
                return;
            }
            boolean updated = mappingStore.setActive(mappingId.trim());
            if (!updated) {
                ctx.status(404).json(Map.of("error", "Mapping not found: " + mappingId));
                return;
            }
            persistActiveMappingId(mappingId.trim(), mappingStoreDir);
            ctx.json(Map.of("message", "Active mapping updated", "activeMappingId", mappingId.trim()));
        });

        app.delete("/mapping", ctx -> {
            String mappingId = ctx.queryParam("id");
            if (mappingId == null || mappingId.isBlank()) {
                ctx.status(400).json(Map.of("error", "Query param 'id' is required"));
                return;
            }
            MappingStore.DeleteResult result = mappingStore.delete(mappingId.trim());
            if (!result.deleted()) {
                ctx.status(404).json(Map.of("error", result.error()));
                return;
            }
            deleteMappingFile(mappingId.trim(), mappingStoreDir);
            if (result.newActiveMappingId() != null) {
                persistActiveMappingId(result.newActiveMappingId(), mappingStoreDir);
            } else {
                deleteActiveMappingFile(mappingStoreDir);
            }
            Map<String, Object> response = new java.util.LinkedHashMap<>();
            response.put("message", "Mapping deleted");
            response.put("deletedMappingId", mappingId.trim());
            response.put("activeMappingId", result.newActiveMappingId());
            ctx.json(response);
        });

        app.get("/mapping/status", ctx -> {
            Map<String, Object> status = new java.util.LinkedHashMap<>();
            status.put("totalMappings", mappingStore.list().size());
            status.put("activeMappingId", mappingStore.activeMappingId().orElse(null));
            mappingStore.getActive().ifPresentOrElse(
                    active -> {
                        status.put("activeMappingName", active.name());
                        status.put("activeVertexLabels", active.config().vertices().keySet());
                        status.put("activeEdgeLabels", active.config().edges().keySet());
                        status.put("activeCreatedAt", active.createdAt());
                    },
                    () -> status.put("activeMappingName", null)
            );
            status.put("mappingStoreDir", mappingStoreDir.toAbsolutePath().toString());
            ctx.json(status);
        });

        app.post("/query", ctx -> {
            try {
                ctx.bodyAsClass(QueryRequest.class);
            } catch (Exception ex) {
                ctx.status(400).json(Map.of("error", "Body must be JSON: {\"gremlin\":\"...\"}"));
                return;
            }

            ctx.status(307).header("Location", "/gremlin/query").json(Map.of(
                    "message", "Query endpoint redirected to Gremlin native execution",
                    "redirectTo", "/gremlin/query",
                    "note", "Use /query/explain for SQL translation details"
            ));
        });

        app.post("/query/explain", ctx -> {
            MappingStore.StoredMapping selectedMapping = resolveMappingForRequest(ctx, mappingStore).orElse(null);
            if (selectedMapping == null) {
                ctx.status(400).json(Map.of("error", "Upload a mapping file first"));
                return;
            }

            QueryRequest request;
            try {
                request = ctx.bodyAsClass(QueryRequest.class);
            } catch (Exception ex) {
                ctx.status(400).json(Map.of("error", "Body must be JSON: {\"gremlin\":\"...\"}"));
                return;
            }

            try {
                // Always include the plan — it's cheap and essential for WCOJ clients
                TranslationResult translationResult =
                        translator.translateWithPlan(request.gremlin(), selectedMapping.config());

                if (isSqlTraceEnabledForRequest(ctx)) {
                    logSqlTranslation("/query/explain", request.gremlin(), translationResult);
                } else {
                    LOG.debug("[SQL-TRACE] endpoint=/query/explain disabled for this request");
                }

                // Determine how this query will actually be executed
                String activeProvider = gremlinExecutionService.providerId();
                boolean isHopQuery = translationResult.plan() != null
                        && translationResult.plan().hops() != null
                        && !translationResult.plan().hops().isEmpty();
                String execEngine;
                String execStrategy;
                if ("wcoj".equals(activeProvider) && isHopQuery) {
                    execEngine   = "WCOJ";
                    execStrategy = "1) Seed SQL: SELECT id FROM <table> WHERE … → start-vertex IDs  |  " +
                                   "2) Leapfrog Trie Join: in-memory DFS over CSR adjacency index (no SQL JOINs)  |  " +
                                   "3) Property-fetch SQL: SELECT id, <props> FROM <table> WHERE id IN (…)  |  " +
                                   "The 'translatedSql' field shows the equivalent N-JOIN plan SQL mode would run — for reference only.";
                } else if ("wcoj".equals(activeProvider)) {
                    execEngine   = "WCOJ_FALLBACK_SQL";
                    execStrategy = "WCOJ fell back to SQL (aggregation/projection query). " +
                                   "The 'translatedSql' field is the exact SQL executed.";
                } else if ("tinkergraph".equals(activeProvider)) {
                    execEngine   = "TINKERGRAPH";
                    execStrategy = "Native TinkerPop traversal — SQL is shown for reference only.";
                } else {
                    execEngine   = "SQL";
                    execStrategy = null;  // SQL mode: translatedSql IS what runs
                }

                QueryExplanation explanation = new QueryExplanation(
                        request.gremlin(),
                        translationResult.sql(),
                        translationResult.parameters(),
                        execEngine,
                        "mappingId=" + selectedMapping.id(),
                        translationResult.plan(),
                        execEngine,
                        execStrategy
                );
                ctx.json(explanation);
            } catch (IllegalArgumentException ex) {
                ctx.status(400).json(Map.of("error", ex.getMessage()));
            }
        });

        app.post("/gremlin/query", ctx -> {
            QueryRequest request;
            try {
                request = ctx.bodyAsClass(QueryRequest.class);
            } catch (Exception ex) {
                ctx.status(400).json(Map.of("error", "Body must be JSON: {\"gremlin\":\"...\"}"));
                return;
            }

            GremlinExecutionService svc = resolveServiceForRequest(ctx, backendRegistry, gremlinExecutionService);
            try {
                logGremlinWithSqlIfAvailable(
                        "/gremlin/query",
                        request.gremlin(),
                        ctx,
                        mappingStore,
                        translator,
                        isSqlTraceEnabledForRequest(ctx)
                );
                GremlinExecutionResult response = svc.execute(request.gremlin());
                ctx.json(response);
            } catch (IllegalArgumentException | ScriptException ex) {
                ctx.status(400).json(Map.of("error", ex.getMessage()));
            }
        });

        app.post("/gremlin/query/tx", ctx -> {
            QueryRequest request;
            try {
                request = ctx.bodyAsClass(QueryRequest.class);
            } catch (Exception ex) {
                ctx.status(400).json(Map.of("error", "Body must be JSON: {\"gremlin\":\"...\"}"));
                return;
            }

            GremlinExecutionService svc = resolveServiceForRequest(ctx, backendRegistry, gremlinExecutionService);
            try {
                logGremlinWithSqlIfAvailable(
                        "/gremlin/query/tx",
                        request.gremlin(),
                        ctx,
                        mappingStore,
                        translator,
                        isSqlTraceEnabledForRequest(ctx)
                );
                GremlinTransactionalExecutionResult response = svc.executeInTransaction(request.gremlin());
                ctx.json(response);
            } catch (IllegalArgumentException | ScriptException ex) {
                ctx.status(400).json(Map.of("error", ex.getMessage()));
            }
        });

        app.get("/gremlin/provider", ctx -> ctx.json(Map.of("provider", gremlinExecutionService.providerId())));

        // List all registered backends (id + url, no credentials).
        app.get("/backends", ctx -> {
            if (backendRegistry == null) {
                ctx.json(Map.of(
                        "backends", List.of(Map.of("id", "default", "url", databaseConfig.url())),
                        "defaultBackend", "default"
                ));
                return;
            }
            ctx.json(Map.of(
                    "backends", backendRegistry.listEntries().stream()
                            .map(e -> Map.of("id", e.id(), "url", e.url()))
                            .toList(),
                    "defaultBackend", backendRegistry.defaultId()
            ));
        });

        /*
         * Register (or replace) a backend at runtime — no JVM restart required.
         * Request body (JSON):
         * <pre>
         * {
         *   "id":       "iceberg",                              // required
         *   "url":      "jdbc:trino://localhost:8080/iceberg",  // required
         *   "user":     "admin",                                // optional
         *   "password": ""                                      // optional
         * }
         * </pre>
         * Returns 200 with the registered backend id and url.
         * Returns 400 if id or url is missing, or if the engine is in tinkergraph mode.
         */
        app.post("/backends/register", ctx -> {
            if (backendRegistry == null) {
                ctx.status(400).json(Map.of(
                        "error", "Runtime backend registration is only supported when the engine " +
                                 "is running in SQL mode (GRAPH_PROVIDER=sql). " +
                                 "Current mode does not use a BackendRegistry."));
                return;
            }

            Map<?, ?> body;
            try {
                body = ctx.bodyAsClass(Map.class);
            } catch (Exception ex) {
                ctx.status(400).json(Map.of("error", "Body must be JSON: {\"id\":\"...\",\"url\":\"...\"}"));
                return;
            }

            String id  = body.get("id")  instanceof String s ? s.trim() : null;
            String url = body.get("url") instanceof String s ? s.trim() : null;

            if (id == null || id.isBlank()) {
                ctx.status(400).json(Map.of("error", "Field 'id' is required"));
                return;
            }
            if (url == null || url.isBlank()) {
                ctx.status(400).json(Map.of("error", "Field 'url' is required"));
                return;
            }

            String user     = body.get("user")     instanceof String s ? s : "";
            String password = body.get("password") instanceof String s ? s : "";

            try {
                BackendRegistry.Entry entry = backendRegistry.register(id, url, user, password, mappingStore);
                LOG.info("Backend registered via API: id={} url={}", entry.id(), entry.url());
                ctx.json(Map.of(
                        "message",  "Backend registered",
                        "id",       entry.id(),
                        "url",      entry.url(),
                        "backends", backendRegistry.listEntries().stream()
                                        .map(e -> Map.of("id", e.id(), "url", e.url()))
                                        .toList()
                ));
            } catch (Exception ex) {
                LOG.error("Failed to register backend id={} url={}: {}", id, url, ex.getMessage(), ex);
                ctx.status(500).json(Map.of("error", "Failed to register backend: " + ex.getMessage()));
            }
        });


        // Allow callers (e.g. the demo module) to register additional routes.
        extensions.accept(app, gremlinExecutionService);

        LOG.info("Graph Query Engine started on port {}", port);
        LOG.info("Database URL: {}", databaseConfig.url());

        return app;
    }

    private static void ensureDatabaseFileInitialized(DatabaseManager databaseManager, DatabaseConfig databaseConfig) {
        try (Connection ignored = databaseManager.connection()) {
            LOG.info("Database connection verified on startup");
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to initialize database: " + databaseConfig.url(), ex);
        }
    }

    private static void logGremlinWithSqlIfAvailable(String endpoint,
                                                     String gremlin,
                                                     Context ctx,
                                                     MappingStore mappingStore,
                                                     GraphQueryTranslator translator,
                                                     boolean sqlTraceEnabledForRequest) {
        LOG.info("[GREMLIN] endpoint={} query={}", endpoint, gremlin);

        if (!sqlTraceEnabledForRequest) {
            LOG.debug("[SQL-TRACE] endpoint={} disabled for this request", endpoint);
            return;
        }

        MappingStore.StoredMapping selectedMapping = resolveMappingForRequest(ctx, mappingStore).orElse(null);
        if (selectedMapping == null) {
            LOG.info("[SQL-TRACE] endpoint={} mapping=missing (upload mapping to enable SQL translation logs)", endpoint);
            return;
        }

        try {
            TranslationResult translation = translator.translate(gremlin, selectedMapping.config());
            logSqlTranslation(endpoint, gremlin, translation);
        } catch (IllegalArgumentException ex) {
            // Not every Gremlin traversal is supported by the SQL translator; keep execution path untouched.
            LOG.info("[SQL-TRACE] endpoint={} unavailable={}", endpoint, ex.getMessage());
        }
    }

    private static void logSqlTranslation(String endpoint, String gremlin, TranslationResult translationResult) {
        LOG.info("[SQL-TRACE] endpoint={} gremlin={}", endpoint, gremlin);
        LOG.info("[SQL-TRACE] sql={}", translationResult.sql());
        LOG.info("[SQL-TRACE] params={}", translationResult.parameters());
    }

    private static boolean isSqlTraceEnabled() {
        String configured = System.getenv().getOrDefault("SQL_TRACE", "true");
        return parseEnabledFlag(configured, true);
    }

    private static boolean isSqlTraceEnabledForRequest(Context ctx) {
        // Priority: ?sql_log=  query param  >  X-SQL-Trace header  >  SQL_TRACE env var
        String queryParam = ctx.queryParam("sql_log");
        if (queryParam != null && !queryParam.isBlank()) {
            return parseEnabledFlag(queryParam, isSqlTraceEnabled());
        }
        String headerValue = ctx.header(SQL_TRACE_HEADER);
        if (headerValue == null || headerValue.isBlank()) {
            return isSqlTraceEnabled();
        }
        return parseEnabledFlag(headerValue, isSqlTraceEnabled());
    }

    private static boolean parseEnabledFlag(String rawValue, boolean defaultValue) {
        if (rawValue == null) {
            return defaultValue;
        }
        String configured = rawValue.trim();
        if (configured.isBlank()) {
            return defaultValue;
        }
        return !"false".equalsIgnoreCase(configured)
                && !"0".equals(configured)
                && !"off".equalsIgnoreCase(configured)
                && !"no".equalsIgnoreCase(configured);
    }

    private static Optional<MappingStore.StoredMapping> resolveMappingForRequest(Context ctx, MappingStore mappingStore) {
        String requestedId = ctx.header(MAPPING_ID_HEADER);
        if (requestedId == null || requestedId.isBlank()) {
            requestedId = ctx.queryParam("mappingId");
        }
        if (requestedId != null && !requestedId.isBlank()) {
            return mappingStore.getById(requestedId.trim());
        }
        return mappingStore.getActive();
    }

    /**
     * Resolves the {@link GremlinExecutionService} for the current request.
     *
     * <p>When an {@code X-Backend-Id} header (or {@code backendId} query param) is present,
     * the specific per-backend service is returned from the registry so that query is executed
     * against that backend directly (bypassing mapping-level routing).
     *
     * <p>When no backend id is specified, the <em>registry-aware</em> {@code fallback} service
     * is returned — this is the {@link com.graphqueryengine.gremlin.provider.SqlGraphProvider} that was constructed with the full
     * {@link BackendRegistry} and therefore routes each query to the correct backend based on
     * the {@code datasource} field in the active mapping, without any explicit header required.
     */
    private static GremlinExecutionService resolveServiceForRequest(
            Context ctx,
            BackendRegistry backendRegistry,
            GremlinExecutionService fallback) {
        if (backendRegistry == null) {
            return fallback;
        }
        String requestedId = ctx.header(BACKEND_ID_HEADER);
        if (requestedId == null || requestedId.isBlank()) {
            requestedId = ctx.queryParam("backendId");
        }
        if (requestedId != null && !requestedId.isBlank()) {
            // Explicit backend override — use that backend's dedicated service directly.
            return backendRegistry.getById(requestedId.trim()).orElse(fallback);
        }
        // No explicit override — use the registry-aware provider so mapping datasource
        // fields automatically route the query to the correct backend.
        return fallback;
    }

    private static void preloadMappingsOnStartup(MappingStore mappingStore,
                                                 ObjectMapper objectMapper,
                                                 Path mappingStoreDir) {
        try {
            Files.createDirectories(mappingStoreDir);
        } catch (IOException ex) {
            LOG.warn("Failed to create mapping store directory {}: {}", mappingStoreDir.toAbsolutePath(), ex.getMessage());
            return;
        }

        String startupMappingPath = System.getenv("MAPPING_PATH");
        if (startupMappingPath != null && !startupMappingPath.isBlank()) {
            Path path = Path.of(startupMappingPath.trim());
            try {
                MappingConfig mapping = readMappingFile(path, objectMapper);
                String name = path.getFileName() == null ? "startup-mapping" : path.getFileName().toString();
                MappingStore.StoredMapping stored = mappingStore.put(null, name, mapping, true);
                persistMapping(stored, objectMapper, mappingStoreDir);
                persistActiveMappingId(stored.id(), mappingStoreDir);
                LOG.info("Startup mapping loaded from MAPPING_PATH={} as id={}", path.toAbsolutePath(), stored.id());
            } catch (Exception ex) {
                LOG.warn("Failed to load startup mapping from MAPPING_PATH={}: {}", path.toAbsolutePath(), ex.getMessage());
            }
        }

        loadPersistedMappings(mappingStore, objectMapper, mappingStoreDir);
        loadPersistedActiveMapping(mappingStore, mappingStoreDir);
    }

    private static void loadPersistedMappings(MappingStore mappingStore,
                                              ObjectMapper objectMapper,
                                              Path mappingStoreDir) {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(mappingStoreDir, "*.json")) {
            for (Path file : files) {
                if (file.getFileName().toString().equals(ACTIVE_MAPPING_FILE)) {
                    continue;
                }
                try {
                    PersistedMappingDocument document = objectMapper.readValue(Files.readAllBytes(file), PersistedMappingDocument.class);
                    mappingStore.put(document.id(), document.name(), document.mapping(), false);
                } catch (Exception ex) {
                    LOG.warn("Skipping invalid mapping file {}: {}", file.toAbsolutePath(), ex.getMessage());
                }
            }
        } catch (IOException ex) {
            LOG.warn("Failed to load persisted mappings from {}: {}", mappingStoreDir.toAbsolutePath(), ex.getMessage());
        }
    }

    private static void loadPersistedActiveMapping(MappingStore mappingStore, Path mappingStoreDir) {
        Path activeFile = mappingStoreDir.resolve(ACTIVE_MAPPING_FILE);
        if (Files.exists(activeFile)) {
            try {
                String activeId = Files.readString(activeFile, StandardCharsets.UTF_8).trim();
                if (!activeId.isBlank() && !mappingStore.setActive(activeId)) {
                    LOG.warn("Persisted active mapping id {} not found in store", activeId);
                }
            } catch (IOException ex) {
                LOG.warn("Failed to read active mapping file {}: {}", activeFile.toAbsolutePath(), ex.getMessage());
            }
        }
    }

    private static MappingConfig readMappingFile(Path path, ObjectMapper objectMapper) throws IOException {
        return objectMapper.readValue(Files.readAllBytes(path), MappingConfig.class);
    }

    private static void persistMapping(MappingStore.StoredMapping stored,
                                       ObjectMapper objectMapper,
                                       Path mappingStoreDir) {
        try {
            Files.createDirectories(mappingStoreDir);
            PersistedMappingDocument document = new PersistedMappingDocument(
                    stored.id(),
                    stored.name(),
                    stored.createdAt(),
                    stored.config()
            );
            byte[] jsonBytes = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(document);
            Files.write(
                    mappingStoreDir.resolve(stored.id() + ".json"),
                    jsonBytes,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
        } catch (Exception ex) {
            LOG.warn("Failed to persist mapping {}: {}", stored.id(), ex.getMessage());
        }
    }

    private static void persistActiveMappingId(String mappingId, Path mappingStoreDir) {
        try {
            Files.createDirectories(mappingStoreDir);
            Files.writeString(
                    mappingStoreDir.resolve(ACTIVE_MAPPING_FILE),
                    mappingId,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
        } catch (IOException ex) {
            LOG.warn("Failed to persist active mapping id {}: {}", mappingId, ex.getMessage());
        }
    }

    private static Path resolveMappingStoreDir() {
        String configured = System.getenv().getOrDefault("MAPPING_STORE_DIR", ".mapping-store");
        return Path.of(configured).toAbsolutePath().normalize();
    }

    private static void deleteMappingFile(String mappingId, Path mappingStoreDir) {
        try {
            Files.deleteIfExists(mappingStoreDir.resolve(mappingId + ".json"));
        } catch (IOException ex) {
            LOG.warn("Failed to delete mapping file for id {}: {}", mappingId, ex.getMessage());
        }
    }

    private static void deleteActiveMappingFile(Path mappingStoreDir) {
        try {
            Files.deleteIfExists(mappingStoreDir.resolve(ACTIVE_MAPPING_FILE));
        } catch (IOException ex) {
            LOG.warn("Failed to delete active mapping file: {}", ex.getMessage());
        }
    }

    private record PersistedMappingDocument(String id, String name, String createdAt, MappingConfig mapping) {
    }
}

