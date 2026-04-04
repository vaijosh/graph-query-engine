package com.graphqueryengine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.graphqueryengine.config.DatabaseConfig;
import com.graphqueryengine.db.DatabaseManager;
import com.graphqueryengine.gremlin.GremlinExecutionResult;
import com.graphqueryengine.gremlin.GremlinExecutionService;
import com.graphqueryengine.gremlin.GremlinTransactionalExecutionResult;
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
    private static final String SQL_TRACE_HEADER = "X-SQL-Trace";
    private static final String MAPPING_ID_HEADER = "X-Mapping-Id";
    private static final String ACTIVE_MAPPING_FILE = ".active-mapping";

    public static void main(String[] args) {
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "7000"));
        start(port);
    }

    public static Javalin start(int port) {
        return start(port, (app, svc) -> {}, new DefaultGraphQueryTranslatorFactory());
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
        DatabaseConfig databaseConfig = DatabaseConfig.fromEnvironment();
        DatabaseManager databaseManager = new DatabaseManager(databaseConfig);
        ensureDatabaseFileInitialized(databaseManager, databaseConfig);
        MappingStore mappingStore = new MappingStore();
        GraphQueryTranslator translator = translatorFactory.create();
        GremlinExecutionService gremlinExecutionService = new GremlinExecutionService();
        ObjectMapper objectMapper = new ObjectMapper();

        Javalin app = Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson());
            config.http.maxRequestSize = 20_000_000L;
        }).start(port);

        Path mappingStoreDir = resolveMappingStoreDir();
        preloadMappingsOnStartup(mappingStore, objectMapper, mappingStoreDir);

        app.get("/health", ctx -> ctx.json(Map.of("status", "ok", "service", "graph-query-engine")));

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

                ctx.status(201).json(Map.of(
                        "message", "Mapping loaded",
                        "mappingId", stored.id(),
                        "mappingName", stored.name(),
                        "active", Objects.equals(mappingStore.activeMappingId().orElse(null), stored.id()),
                        "vertexLabels", mappingConfig.vertices().keySet(),
                        "edgeLabels", mappingConfig.edges().keySet(),
                        "createdAt", stored.createdAt()
                ));
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
                // Check if caller requested planner-stage output via ?plan=true query param
                boolean includePlan = parseEnabledFlag(ctx.queryParam("plan"), false);
                TranslationResult translationResult = includePlan
                        ? translator.translateWithPlan(request.gremlin(), selectedMapping.config())
                        : translator.translate(request.gremlin(), selectedMapping.config());

                if (isSqlTraceEnabledForRequest(ctx)) {
                    logSqlTranslation("/query/explain", request.gremlin(), translationResult);
                } else {
                    LOG.debug("[SQL-TRACE] endpoint=/query/explain disabled for this request");
                }
                QueryExplanation explanation = new QueryExplanation(
                        request.gremlin(),
                        translationResult.sql(),
                        translationResult.parameters(),
                        "SQL_EXPLAIN",
                        "This endpoint shows SQL translation for reference. Use /gremlin/query for actual execution. mappingId=" + selectedMapping.id(),
                        translationResult.plan()
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

            try {
                logGremlinWithSqlIfAvailable(
                        "/gremlin/query",
                        request.gremlin(),
                        ctx,
                        mappingStore,
                        translator,
                        isSqlTraceEnabledForRequest(ctx)
                );
                GremlinExecutionResult response = gremlinExecutionService.execute(request.gremlin());
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

            try {
                logGremlinWithSqlIfAvailable(
                        "/gremlin/query/tx",
                        request.gremlin(),
                        ctx,
                        mappingStore,
                        translator,
                        isSqlTraceEnabledForRequest(ctx)
                );
                GremlinTransactionalExecutionResult response = gremlinExecutionService.executeInTransaction(request.gremlin());
                ctx.json(response);
            } catch (IllegalArgumentException | ScriptException ex) {
                ctx.status(400).json(Map.of("error", ex.getMessage()));
            }
        });

        app.get("/gremlin/provider", ctx -> ctx.json(Map.of("provider", gremlinExecutionService.providerId())));


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

