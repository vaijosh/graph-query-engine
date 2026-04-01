package com.graphqueryengine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.graphqueryengine.config.DatabaseConfig;
import com.graphqueryengine.db.DatabaseManager;
import com.graphqueryengine.gremlin.GremlinExecutionResult;
import com.graphqueryengine.gremlin.GremlinExecutionService;
import com.graphqueryengine.gremlin.GremlinTransactionalExecutionResult;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.MappingStore;
import com.graphqueryengine.query.GremlinSqlTranslator;
import com.graphqueryengine.query.QueryRequest;
import com.graphqueryengine.query.QueryExplanation;
import com.graphqueryengine.query.TranslationResult;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.UploadedFile;
import io.javalin.json.JavalinJackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphqueryengine.gremlin.provider.TinkerGraphProvider;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
        return start(port, (app, svc) -> {});
    }

    /**
     * Starts the engine and invokes {@code extensions} so callers (e.g. the demo module)
     * can register additional routes against the same {@link Javalin} instance without
     * creating a dependency from the engine onto demo code.
     */
    public static Javalin start(int port, java.util.function.BiConsumer<Javalin, GremlinExecutionService> extensions) {
        DatabaseConfig databaseConfig = DatabaseConfig.fromEnvironment();
        DatabaseManager databaseManager = new DatabaseManager(databaseConfig);
        ensureDatabaseFileInitialized(databaseManager, databaseConfig);
        MappingStore mappingStore = new MappingStore();
        GremlinSqlTranslator translator = new GremlinSqlTranslator();
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
                TranslationResult translationResult = translator.translate(request.gremlin(), selectedMapping.config());
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
                        "This endpoint shows SQL translation for reference. Use /gremlin/query for actual execution. mappingId=" + selectedMapping.id()
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

        app.post("/admin/load-aml-csv", ctx -> {
            String pathParam = ctx.queryParam("path");
            if (pathParam == null || pathParam.isBlank()) {
                ctx.status(400).json(Map.of("error", "Query param 'path' is required"));
                return;
            }
            int maxRows;
            try {
                maxRows = Integer.parseInt(ctx.queryParamAsClass("maxRows", String.class).getOrDefault("100000"));
            } catch (Exception ex) {
                maxRows = 100_000;
            }

            Path csvPath = Path.of(pathParam.trim());
            if (!Files.exists(csvPath)) {
                ctx.status(400).json(Map.of("error", "CSV file not found: " + csvPath.toAbsolutePath()));
                return;
            }

            if (!(gremlinExecutionService.provider() instanceof TinkerGraphProvider tinkerProvider)) {
                ctx.status(400).json(Map.of("error", "load-aml-csv requires the TinkerGraph provider"));
                return;
            }

            try {
                TinkerGraph graph = tinkerProvider.getGraph();
                // Collect edges first (must be removed before vertices to avoid dangling refs)
                List<org.apache.tinkerpop.gremlin.structure.Edge> allEdges = new ArrayList<>();
                graph.edges().forEachRemaining(allEdges::add);
                allEdges.forEach(org.apache.tinkerpop.gremlin.structure.Element::remove);
                // Collect vertices, then remove
                List<Vertex> allVertices = new ArrayList<>();
                graph.vertices().forEachRemaining(allVertices::add);
                allVertices.forEach(org.apache.tinkerpop.gremlin.structure.Element::remove);

                // ── ID generators ────────────────────────────────────────────
                AtomicLong vid = new AtomicLong(1);
                AtomicLong eid = new AtomicLong(10_000_000);

                // ── Vertex registries ─────────────────────────────────────────
                HashMap<String, Vertex> bankMap    = new HashMap<>();  // bankId      → Bank
                HashMap<String, Vertex> accountMap = new HashMap<>();  // bank:acctId → Account
                HashMap<String, Vertex> countryMap = new HashMap<>();  // countryCode → Country
                HashMap<String, Vertex> txMap      = new HashMap<>();  // transactionId → Transaction

                // ── Edge de-dup sets ─────────────────────────────────────────
                java.util.Set<String> belongsToSeen = new java.util.HashSet<>();
                java.util.Set<String> locatedInSeen = new java.util.HashSet<>();
                java.util.Set<String> sentViaSeen   = new java.util.HashSet<>();

                // ── Static country table (derived from bankId hash) ──────────
                String[][] COUNTRIES = {
                    {"US","United States","LOW",   "Americas",    "false"},
                    {"GB","United Kingdom","LOW",  "Europe",      "false"},
                    {"DE","Germany",       "LOW",  "Europe",      "false"},
                    {"CH","Switzerland",   "MEDIUM","Europe",     "false"},
                    {"HK","Hong Kong",     "MEDIUM","Asia",       "false"},
                    {"SG","Singapore",     "LOW",  "Asia",        "false"},
                    {"AE","UAE",           "MEDIUM","Middle East","false"},
                    {"NG","Nigeria",       "HIGH", "Africa",      "false"},
                    {"KY","Cayman Islands","HIGH", "Americas",    "true"},
                    {"PA","Panama",        "HIGH", "Americas",    "true"},
                };

                // Pre-create Country vertices
                for (String[] c : COUNTRIES) {
                    Vertex cv = graph.addVertex(
                            T.id, vid.getAndIncrement(), T.label, "Country",
                            "countryCode",   c[0], "countryName",   c[1],
                            "riskLevel",     c[2], "region",        c[3],
                            "fatfBlacklist", c[4]);
                    countryMap.put(c[0], cv);
                }

                AtomicInteger rowsLoaded    = new AtomicInteger(0);
                AtomicInteger alertsCreated = new AtomicInteger(0);

                CSVFormat fmt = CSVFormat.DEFAULT.builder()
                        .setHeader().setSkipHeaderRecord(true).setTrim(true).get();

                try (Reader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8);
                     CSVParser parser = fmt.parse(reader)) {

                    for (CSVRecord rec : parser) {
                        if (rowsLoaded.get() >= maxRows) break;

                        String fromBankId = rec.get("from_bank");
                        String fromAcctId = rec.get("from_account");
                        String toBankId   = rec.get("to_bank");
                        String toAcctId   = rec.get("to_account");
                        String amount     = rec.get("amount_paid");
                        String currency   = rec.get("payment_currency");
                        String format     = rec.get("payment_format");
                        String ts         = rec.get("timestamp");
                        String laundering = rec.get("is_laundering");
                        String txId       = rec.get("transaction_id");

                        // Derive country from bankId hash
                        String fromCC = COUNTRIES[Math.abs(fromBankId.hashCode()) % COUNTRIES.length][0];
                        String toCC   = COUNTRIES[Math.abs(toBankId.hashCode())   % COUNTRIES.length][0];

                        // ── Bank vertices ────────────────────────────────────
                        Vertex fromBank = bankMap.computeIfAbsent(fromBankId, k -> graph.addVertex(
                                T.id, vid.getAndIncrement(), T.label, "Bank",
                                "bankId", k, "bankName", "Bank-" + k,
                                "countryCode", fromCC,
                                "swiftCode", "SW" + k.toUpperCase(),
                                "tier", String.valueOf(Math.abs(k.hashCode()) % 3 + 1)));

                        Vertex toBank = bankMap.computeIfAbsent(toBankId, k -> graph.addVertex(
                                T.id, vid.getAndIncrement(), T.label, "Bank",
                                "bankId", k, "bankName", "Bank-" + k,
                                "countryCode", toCC,
                                "swiftCode", "SW" + k.toUpperCase(),
                                "tier", String.valueOf(Math.abs(k.hashCode()) % 3 + 1)));

                        // ── Account vertices ─────────────────────────────────
                        String fromKey = fromBankId + ":" + fromAcctId;
                        String toKey   = toBankId   + ":" + toAcctId;
                        boolean suspicious = "1".equals(laundering);

                        Vertex fromAcct = accountMap.computeIfAbsent(fromKey, k -> graph.addVertex(
                                T.id, vid.getAndIncrement(), T.label, "Account",
                                "accountId",   fromAcctId, "bankId", fromBankId,
                                "accountType", Math.abs(fromAcctId.hashCode()) % 2 == 0 ? "CORPORATE" : "PERSONAL",
                                "riskScore",   suspicious ? "0.85" : "0.15",
                                "isBlocked",   "false",
                                "openedDate",  "2020-01-01"));

                        Vertex toAcct = accountMap.computeIfAbsent(toKey, k -> graph.addVertex(
                                T.id, vid.getAndIncrement(), T.label, "Account",
                                "accountId",   toAcctId, "bankId", toBankId,
                                "accountType", Math.abs(toAcctId.hashCode()) % 2 == 0 ? "CORPORATE" : "PERSONAL",
                                "riskScore",   suspicious ? "0.75" : "0.10",
                                "isBlocked",   "false",
                                "openedDate",  "2020-01-01"));

                        // ── Transaction vertex ───────────────────────────────
                        Vertex txVertex = txMap.computeIfAbsent(txId, k -> graph.addVertex(
                                T.id, vid.getAndIncrement(), T.label, "Transaction",
                                "transactionId", k,
                                "amount",        amount,
                                "currency",      currency,
                                "paymentFormat", format,
                                "eventTime",     ts,
                                "isLaundering",  laundering,
                                "channel",       format.toLowerCase().contains("wire") ? "WIRE" : "DIGITAL"));

                        // ── TRANSFER edge: Account → Account ─────────────────
                        fromAcct.addEdge("TRANSFER", toAcct,
                                T.id, eid.getAndIncrement(),
                                "transactionId", txId, "amount", amount,
                                "currency", currency, "paymentFormat", format,
                                "eventTime", ts, "isLaundering", laundering);

                        // ── BELONGS_TO edges: Account → Bank (once per pair) ──
                        if (belongsToSeen.add(fromKey)) {
                            fromAcct.addEdge("BELONGS_TO", fromBank,
                                    T.id, eid.getAndIncrement(),
                                    "since", "2020-01-01", "isPrimary", "true");
                        }
                        if (belongsToSeen.add(toKey)) {
                            toAcct.addEdge("BELONGS_TO", toBank,
                                    T.id, eid.getAndIncrement(),
                                    "since", "2020-01-01", "isPrimary", "true");
                        }

                        // ── LOCATED_IN edges: Bank → Country (once per bank) ──
                        Vertex fromCountryV = countryMap.get(fromCC);
                        Vertex toCountryV   = countryMap.get(toCC);
                        if (fromCountryV != null && locatedInSeen.add(fromBankId)) {
                            fromBank.addEdge("LOCATED_IN", fromCountryV,
                                    T.id, eid.getAndIncrement(), "isHeadquarters", "true");
                        }
                        if (toCountryV != null && locatedInSeen.add(toBankId)) {
                            toBank.addEdge("LOCATED_IN", toCountryV,
                                    T.id, eid.getAndIncrement(), "isHeadquarters", "true");
                        }

                        // ── RECORDED_AS edge: Account → Transaction ──────────
                        // Link the sending account to its transaction record
                        fromAcct.addEdge("RECORDED_AS", txVertex,
                                T.id, eid.getAndIncrement(),
                                "recordedAt", ts, "source", "CSV_LOAD");

                        // ── SENT_VIA edge: Account → Country (once per pair) ──
                        if (toCountryV != null && sentViaSeen.add(fromKey + "->" + toCC)) {
                            fromAcct.addEdge("SENT_VIA", toCountryV,
                                    T.id, eid.getAndIncrement(),
                                    "channelType", format, "routedAt", ts);
                        }

                        // ── FLAGGED_BY edge: Account → Alert (suspicious only) ─
                        if (suspicious) {
                            String severity = Double.parseDouble(amount) > 50_000 ? "HIGH" : "MEDIUM";
                            Vertex alert = graph.addVertex(
                                    T.id, vid.getAndIncrement(), T.label, "Alert",
                                    "alertId",   "ALERT-" + txId,
                                    "alertType", "SUSPICIOUS_TRANSFER",
                                    "severity",  severity,
                                    "status",    "OPEN",
                                    "raisedAt",  ts);
                            fromAcct.addEdge("FLAGGED_BY", alert,
                                    T.id, eid.getAndIncrement(),
                                    "flaggedAt", ts,
                                    "reason",    "Suspicious outbound transfer");
                            alertsCreated.incrementAndGet();
                        }

                        rowsLoaded.incrementAndGet();
                    }
                }

                Map<String, Object> result = new java.util.LinkedHashMap<>();
                result.put("rowsLoaded",         rowsLoaded.get());
                result.put("accountsCreated",    accountMap.size());
                result.put("banksCreated",        bankMap.size());
                result.put("countriesCreated",    countryMap.size());
                result.put("transactionsCreated", txMap.size());
                result.put("alertsCreated",       alertsCreated.get());
                result.put("transfersCreated",    rowsLoaded.get());
                result.put("sourcePath",          csvPath.toAbsolutePath().toString());
                result.put("maxRows",             maxRows);
                result.put("provider",            gremlinExecutionService.providerId());
                ctx.status(200).json(result);

            } catch (Exception ex) {
                LOG.error("Failed to load AML CSV: {}", ex.getMessage(), ex);
                ctx.status(500).json(Map.of("error", "Failed to load CSV: " + ex.getMessage()));
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
                                                     GremlinSqlTranslator translator,
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

