package com.graphqueryengine.gremlin.provider;

import com.graphqueryengine.config.DatabaseConfig;
import com.graphqueryengine.db.DatabaseManager;
import com.graphqueryengine.engine.wcoj.AdjacencyIndexRegistry;
import com.graphqueryengine.engine.wcoj.WcojOptimiser;
import com.graphqueryengine.gremlin.GremlinExecutionResult;
import com.graphqueryengine.gremlin.GremlinTransactionalExecutionResult;
import com.graphqueryengine.mapping.BackendConnectionConfig;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.MappingStore;
import com.graphqueryengine.query.api.GraphQueryTranslator;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.factory.DefaultGraphQueryTranslatorFactory;

import javax.script.ScriptException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A {@link GraphProvider} that executes Gremlin queries by translating them to SQL
 * and running them directly against a JDBC database (e.g. H2, Trino / Iceberg).
 *
 * <h3>WCOJ acceleration</h3>
 * <p>When {@code WCOJ_ENABLED=true} (the default), multi-hop path traversals are
 * intercepted by the embedded {@link WcojOptimiser} before falling back to SQL.
 * Set {@code WCOJ_ENABLED=false} to disable the optimiser and always use SQL.
 * Tuning knobs: {@code WCOJ_MAX_EDGES} (in-memory edge limit) and
 * {@code WCOJ_DISK_QUOTA_MB} (per-mapping disk quota, default 2048 MB).
 *
 * <h3>Single-backend mode</h3>
 * <p>Constructed with a single {@link DatabaseManager} — all queries go to that one connection.
 *
 * <h3>Multi-backend / hybrid mode</h3>
 * <p>Constructed with a {@link BackendRegistry} — the connection is chosen per query based
 * on the {@code datasource} field of the active mapping's vertex or edge entry
 * (falling back to {@link MappingConfig#defaultDatasource()}, then to the registry default).
 * This allows a single Gremlin query to reference tables that live in different backends.
 */
public class SqlGraphProvider implements GraphProvider {

    private static final Logger LOG = Logger.getLogger(SqlGraphProvider.class.getName());

    private final DatabaseManager databaseManager;   // null when registry is set
    private final BackendRegistry backendRegistry;   // null in single-backend mode
    private final GraphQueryTranslator translator;
    private final MappingStore mappingStore;
    /** Non-null when WCOJ_ENABLED=true (the default). */
    private final WcojOptimiser wcojOptimiser;
    /**
     * Cache of DatabaseManager instances keyed by JDBC URL — reused by the WCOJ optimiser
     * when the active mapping declares its own backends (e.g. Trino for Iceberg).
     */
    private final ConcurrentHashMap<String, DatabaseManager> managerCache = new ConcurrentHashMap<>();

    // ── Constructors ─────────────────────────────────────────────────────────

    /** Single-backend constructor (legacy / default). */
    public SqlGraphProvider(DatabaseManager databaseManager, MappingStore mappingStore) {
        this.databaseManager  = databaseManager;
        this.backendRegistry  = null;
        this.mappingStore     = mappingStore;
        this.translator       = new DefaultGraphQueryTranslatorFactory().create();
        this.wcojOptimiser    = buildWcojOptimiser(mappingStore);
    }

    /** Multi-backend constructor: connection is resolved per-query from the registry. */
    public SqlGraphProvider(BackendRegistry backendRegistry, MappingStore mappingStore) {
        this.databaseManager  = null;
        this.backendRegistry  = backendRegistry;
        this.mappingStore     = mappingStore;
        this.translator       = new DefaultGraphQueryTranslatorFactory().create();
        this.wcojOptimiser    = buildWcojOptimiser(mappingStore);
    }

    /**
     * Test-injectable constructor: supply an explicit {@link WcojOptimiser}
     * (or {@code null} to disable WCOJ regardless of {@code WCOJ_ENABLED}).
     */
    public SqlGraphProvider(DatabaseManager databaseManager, MappingStore mappingStore,
                            WcojOptimiser wcojOptimiser) {
        this.databaseManager  = databaseManager;
        this.backendRegistry  = null;
        this.mappingStore     = mappingStore;
        this.translator       = new DefaultGraphQueryTranslatorFactory().create();
        this.wcojOptimiser    = wcojOptimiser;
    }

    /** Reads {@code WCOJ_ENABLED} / tuning env-vars and wires up the optimiser (or returns null). */
    private static WcojOptimiser buildWcojOptimiser(MappingStore mappingStore) {
        String enabled = System.getenv().getOrDefault("WCOJ_ENABLED", "true").trim().toLowerCase();
        if ("false".equals(enabled) || "0".equals(enabled) || "off".equals(enabled)) {
            LOG.info("[WCOJ] Disabled via WCOJ_ENABLED=" + enabled);
            return null;
        }

        long maxEdgesInMemory = AdjacencyIndexRegistry.DEFAULT_MAX_EDGES_IN_MEMORY;
        String maxEdgesEnv = System.getenv("WCOJ_MAX_EDGES");
        if (maxEdgesEnv != null && !maxEdgesEnv.isBlank()) {
            try { maxEdgesInMemory = Long.parseLong(maxEdgesEnv.trim()); }
            catch (NumberFormatException ignored) { /* keep default */ }
        }

        long diskQuotaBytes = 2048L * 1024 * 1024; // 2 GB default
        String diskQuotaEnv = System.getenv("WCOJ_DISK_QUOTA_MB");
        if (diskQuotaEnv != null && !diskQuotaEnv.isBlank()) {
            try { diskQuotaBytes = Long.parseLong(diskQuotaEnv.trim()) * 1024 * 1024; }
            catch (NumberFormatException ignored) { /* keep default */ }
        }

        // TTL — default 30 s; set WCOJ_INDEX_TTL_SECONDS=0 to disable expiry.
        long ttlSeconds = AdjacencyIndexRegistry.DEFAULT_TTL_SECONDS;
        String ttlEnv = System.getenv("WCOJ_INDEX_TTL_SECONDS");
        if (ttlEnv != null && !ttlEnv.isBlank()) {
            try { ttlSeconds = Long.parseLong(ttlEnv.trim()); }
            catch (NumberFormatException ignored) { /* keep default */ }
        }

        AdjacencyIndexRegistry registry = new AdjacencyIndexRegistry(
                maxEdgesInMemory, diskQuotaBytes, AdjacencyIndexRegistry.DEFAULT_CACHE_DIR, ttlSeconds);
        LOG.info("[WCOJ] Enabled — maxEdgesInMemory=" + maxEdgesInMemory
                + " diskQuotaBytes=" + diskQuotaBytes
                + " indexTtlSeconds=" + ttlSeconds);
        return new WcojOptimiser(mappingStore, registry);
    }

    // ── Public accessors ─────────────────────────────────────────────────────

    /** Returns the single {@link DatabaseManager}, or {@code null} in multi-backend mode. */
    public DatabaseManager databaseManager() {
        return databaseManager;
    }

    @Override
    public String providerId() {
        return backendRegistry != null ? ProviderConstants.PROVIDER_SQL_MULTI : ProviderConstants.PROVIDER_SQL;
    }

    // ── Execution ────────────────────────────────────────────────────────────

    // sql is always produced by SqlTranslator from a parsed Gremlin AST, never from
    // raw user input. All user-supplied values travel via the `params` list and are
    // bound with ps.setObject() — no SQL injection risk. Suppress IDE dataflow warning.
    @Override
    public GremlinExecutionResult execute(String gremlin) throws ScriptException {
        MappingConfig mappingConfig = mappingStore.getActive()
                .map(MappingStore.StoredMapping::config)
                .orElse(null);

        if (mappingConfig == null) {
            return new GremlinExecutionResult(gremlin, List.of(), 0);
        }

        // ── WCOJ fast path (multi-hop path/count traversals) ─────────────────
        if (wcojOptimiser != null) {
            DatabaseManager wcojDm = resolveManagerForConfig(mappingConfig);
            WcojOptimiser.Result wcojResult = wcojOptimiser.tryExecute(gremlin, mappingConfig, wcojDm);
            if (wcojResult != null) {
                return wcojResult.executionResult();
            }
        }

        // ── SQL path ─────────────────────────────────────────────────────────
        TranslationResult translationResult;
        try {
            translationResult = translator.translate(gremlin, mappingConfig);
        } catch (IllegalArgumentException ex) {
            throw new ScriptException("SQL translation failed: " + ex.getMessage());
        }

        String sql = translationResult.sql();
        List<Object> params = translationResult.parameters();

        // Resolve which JDBC connection to use for this query.
        // Throws ScriptException if the required backend is not yet registered.
        DatabaseManager dm = resolveManager(translationResult, mappingConfig);

        assert sql != null;
        try (Connection conn = dm.connection()) {
            // Materialise any _degN inline aggregates into session temp tables
            // so H2 can't re-correlate them back into per-row scans.
            String rewrittenSql = materialiseDegreeSubqueries(conn, sql);

            try (PreparedStatement ps = conn.prepareStatement(rewrittenSql)) {
                for (int i = 0; i < params.size(); i++) {
                    ps.setObject(i + 1, params.get(i));
                }

                try (ResultSet rs = ps.executeQuery()) {
                    ResultSetMetaData meta = rs.getMetaData();
                    int cols = meta.getColumnCount();
                    List<Object> results = new ArrayList<>();

                    // Detect if this is a path query (column names end with a digit: name0, name1, ...)
                    boolean isPath = cols > 1;
                    for (int c = 1; c <= cols && isPath; c++) {
                        if (!meta.getColumnLabel(c).matches(".+\\d+$")) isPath = false;
                    }

                    while (rs.next()) {
                        if (cols == 1) {
                            results.add(rs.getObject(1));
                        } else if (isPath) {
                            List<Object> path = new ArrayList<>();
                            for (int c = 1; c <= cols; c++) path.add(rs.getObject(c));
                            results.add(path);
                        } else {
                            Map<String, Object> row = new LinkedHashMap<>();
                            for (int c = 1; c <= cols; c++) {
                                Object val = rs.getObject(c);
                                row.put(meta.getColumnLabel(c), val == null ? null : List.of(val));
                            }
                            results.add(row);
                        }
                    }

                    return new GremlinExecutionResult(gremlin, results, results.size());
                }
            }
        } catch (SQLException ex) {
            throw new ScriptException("SQL execution error: " + ex.getMessage());
        }
    }

    /**
     * Executes {@code gremlin} as a read-only query and wraps the result with transaction
     * metadata fields for clients that expect them.
     *
     * <p><strong>This method does NOT open a real JDBC transaction.</strong>
     * It calls {@link #execute(String)} (which runs in auto-commit mode) and then
     * decorates the result with hardcoded {@code transactionMode="read-only"} and
     * {@code transactionStatus="committed"}.
     *
     * <p>Write operations ({@code addV}, {@code addE}, {@code property}, {@code drop})
     * are not supported by the SQL translator and will throw {@link ScriptException}.
     */
    @Override
    public GremlinTransactionalExecutionResult executeInTransaction(String gremlin) throws ScriptException {
        GremlinExecutionResult result = execute(gremlin);
        return new GremlinTransactionalExecutionResult(
                result.gremlin(), result.results(), result.resultCount(),
                ProviderConstants.TX_MODE_READ_ONLY, ProviderConstants.TX_STATUS_COMMITTED);
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /**
     * Matches inline degree-aggregate subqueries of the form:
     * <pre>
     *   LEFT JOIN (SELECT &lt;joinCol&gt; AS _id, COUNT(*) AS _cnt
     *              FROM &lt;table&gt; [WHERE ...] GROUP BY &lt;joinCol&gt;
     *              LIMIT 2147483647) _degN
     *          ON _degN._id = ...
     * </pre>
     * The LIMIT fence prevents H2's predicate-pushdown only in embedded mode.
     * In {@code AUTO_SERVER} (remote) mode H2 may strip it and re-correlate the
     * GROUP BY into a per-row scan, causing O(N²) runtimes on large datasets.
     *
     * <p>This method materialises every such subquery into a session-scoped
     * H2 local temporary table ({@code DROP TABLE IF EXISTS} + {@code CREATE ... AS SELECT})
     * using the <em>same</em> connection that will run the main query.  The main SQL is
     * then rewritten to reference those temp tables directly, so H2 never sees the
     * inline aggregation at all.
     *
     * <p>For non-H2 dialects (Trino, DuckDB, etc.) the SQL will not contain the
     * {@code LIMIT 2147483647} sentinel inside a {@code _deg\d+} alias, so the pattern
     * will not match and the original SQL is returned unchanged.
     */
    private static final Pattern DEG_SUBQUERY_PATTERN = Pattern.compile(
            "LEFT JOIN \\((.+?LIMIT 2147483647)\\) (_deg\\d+)\\s+ON",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static String materialiseDegreeSubqueries(Connection conn, String sql) throws SQLException {
        Matcher m = DEG_SUBQUERY_PATTERN.matcher(sql);
        if (!m.find()) return sql;  // no degree aggregates — nothing to do

        // Rebuild from the start
        m.reset();
        StringBuilder rewritten = new StringBuilder();
        int lastEnd = 0;

        try (Statement stmt = conn.createStatement()) {
            while (m.find()) {
                String aggSql  = m.group(1); // e.g. SELECT from_account_id AS _id, COUNT(*) ...
                String alias   = m.group(2); // e.g. _deg0

                // Drop any leftover temp table from a previous (possibly failed) call in this session
                stmt.execute("DROP TABLE IF EXISTS " + alias);
                // Materialise: CREATE LOCAL TEMPORARY TABLE _degN NOT PERSISTENT AS SELECT ...
                stmt.execute("CREATE LOCAL TEMPORARY TABLE " + alias
                        + " NOT PERSISTENT AS " + aggSql);
                // Index on _id so the subsequent LEFT JOIN is a fast lookup, not an O(N²) scan
                stmt.execute("CREATE INDEX " + alias + "_idx ON " + alias + "(_id)");

                // Rewrite: replace  LEFT JOIN (SELECT ... LIMIT 2147483647) _degN  ON
                //      with        LEFT JOIN _degN  ON
                rewritten.append(sql, lastEnd, m.start());
                rewritten.append("LEFT JOIN ").append(alias).append(" ON");
                lastEnd = m.end();
            }
        }
        rewritten.append(sql, lastEnd, sql.length());
        return rewritten.toString();
    }

    /**
     * Resolves the {@link DatabaseManager} to use for a translated query.
     *
     * <p>In single-backend mode the fixed manager is always returned.
     *
     * <p>In multi-backend mode the datasource is inferred from the mapping in this order:
     * <ol>
     *   <li>Per-vertex/edge {@code datasource} field matched against the generated SQL</li>
     *   <li>{@link MappingConfig#defaultDatasource()}</li>
     *   <li>Registry default (first-registered backend)</li>
     * </ol>
     *
     * <p>If the inferred datasource id is not yet registered (e.g. the backend was not
     * configured at startup), a {@link ScriptException} is thrown with a clear message
     * instead of silently routing to the wrong backend.
     */
    private DatabaseManager resolveManager(TranslationResult translationResult,
                                           MappingConfig mappingConfig) throws ScriptException {
        if (backendRegistry == null) {
            return databaseManager;
        }
        String datasource = inferDatasource(translationResult, mappingConfig);
        if (datasource != null && !backendRegistry.contains(datasource)) {
            throw new ScriptException(
                    "Backend '" + datasource + "' required by the active mapping is not registered. " +
                    "Register it first via POST /backends/register with id='" + datasource + "'.");
        }
        // datasource == null → falls back to registry default (first-registered backend).
        return backendRegistry.getManagerById(datasource);
    }

    /**
     * Resolves the best {@link DatabaseManager} for the given mapping config, used by the
     * WCOJ optimiser (which runs before SQL translation, so there is no TranslationResult yet).
     *
     * <p>Priority:
     * <ol>
     *   <li>Mapping-declared backend keyed by {@link MappingConfig#defaultDatasource()}.</li>
     *   <li>First entry in {@code config.backends()} (fallback when no defaultDatasource is set).</li>
     *   <li>BackendRegistry default (multi-backend mode).</li>
     *   <li>Fixed single {@link DatabaseManager} (single-backend mode).</li>
     * </ol>
     *
     * <p>When the resolved backend URL is a Trino URL ({@code jdbc:trino:}), the Iceberg
     * warehouse optimizer is automatically activated by appending the following Trino session
     * properties to the JDBC URL (if not already present):
     * <ul>
     *   <li>{@code sessionProperties.optimizer.join_reordering_strategy=AUTOMATIC}</li>
     *   <li>{@code sessionProperties.optimizer.optimize_hash_generation=true}</li>
     *   <li>{@code sessionProperties.iceberg.pushdown_filter_enabled=true}</li>
     * </ul>
     */
    private DatabaseManager resolveManagerForConfig(MappingConfig config) {
        if (config != null && config.backends() != null && !config.backends().isEmpty()) {
            // Prefer the backend that matches defaultDatasource; fall back to first entry.
            BackendConnectionConfig bcc = null;
            String defaultDs = config.defaultDatasource();
            if (defaultDs != null && !defaultDs.isBlank()) {
                bcc = config.backends().get(defaultDs);
            }
            if (bcc == null) {
                bcc = config.backends().values().iterator().next();
            }

            if (bcc != null && bcc.url() != null && !bcc.url().isBlank()) {
                String resolvedUrl = applyIcebergWarehouseOptimizer(bcc.url());
                final BackendConnectionConfig finalBcc = bcc;
                return managerCache.computeIfAbsent(resolvedUrl, url -> {
                    LOG.info("[WCOJ] Using mapping backend connection: " + url);
                    return new DatabaseManager(
                            new DatabaseConfig(url, finalBcc.user(), finalBcc.password(),
                                    finalBcc.driverClass()));
                });
            }
        }
        if (backendRegistry != null) {
            return backendRegistry.getManagerById(null); // → registry default
        }
        return databaseManager;
    }

    /**
     * When {@code url} is a Trino JDBC URL, appends Trino session properties that
     * activate the Iceberg connector's cost-based warehouse optimizer.  Returns the
     * original URL unchanged for non-Trino backends.
     *
     * <p>The following session properties are injected (skipped if already present):
     * <ul>
     *   <li>{@code sessionProperties.optimizer.join_reordering_strategy=AUTOMATIC}</li>
     *   <li>{@code sessionProperties.optimizer.optimize_hash_generation=true}</li>
     *   <li>{@code sessionProperties.iceberg.pushdown_filter_enabled=true}</li>
     * </ul>
     */
    static String applyIcebergWarehouseOptimizer(String url) {
        if (url == null || !url.startsWith("jdbc:trino:")) {
            return url;
        }
        // Trino JDBC uses "?" for the first parameter and "&" for subsequent ones.
        String separator = url.contains("?") ? "&" : "?";
        StringBuilder sb = new StringBuilder(url);
        if (!url.contains("optimizer.join_reordering_strategy")) {
            sb.append(separator).append("sessionProperties.optimizer.join_reordering_strategy=AUTOMATIC");
            separator = "&";
        }
        if (!url.contains("optimizer.optimize_hash_generation")) {
            sb.append(separator).append("sessionProperties.optimizer.optimize_hash_generation=true");
            separator = "&";
        }
        if (!url.contains("iceberg.pushdown_filter_enabled")) {
            sb.append(separator).append("sessionProperties.iceberg.pushdown_filter_enabled=true");
        }
        String result = sb.toString();
        if (!result.equals(url)) {
            LOG.info("[WCOJ] Iceberg warehouse optimizer session properties applied to Trino URL.");
        }
        return result;
    }

    /**
     * Infers the datasource id from the SQL translation result by matching the generated
     * SQL's table reference back to vertex/edge mappings.
     *
     * <p>Strategy: scan vertex mappings first (most queries are vertex-focused), then edges.
     * Return the first match whose resolved table reference appears in the SQL string.
     * Falls back to {@link MappingConfig#defaultDatasource()}, then {@code null} (registry default).
     */
    private static String inferDatasource(TranslationResult translationResult,
                                          MappingConfig mappingConfig) {
        String sql = translationResult.sql().toLowerCase();

        for (Map.Entry<String, com.graphqueryengine.mapping.VertexMapping> e
                : mappingConfig.vertices().entrySet()) {
            com.graphqueryengine.mapping.VertexMapping vm = e.getValue();
            String ref = vm.table().toLowerCase();
            if (sql.contains(ref)) {
                String ds = vm.datasource() != null ? vm.datasource()
                        : mappingConfig.defaultDatasource();
                if (ds != null) return ds;
            }
        }

        for (Map.Entry<String, com.graphqueryengine.mapping.EdgeMapping> e
                : mappingConfig.edges().entrySet()) {
            com.graphqueryengine.mapping.EdgeMapping em = e.getValue();
            String ref = em.table().toLowerCase();
            if (sql.contains(ref)) {
                String ds = em.datasource() != null ? em.datasource()
                        : mappingConfig.defaultDatasource();
                if (ds != null) return ds;
            }
        }

        // Fall back to mapping-level default, then null (→ caller uses registry default).
        return mappingConfig.defaultDatasource();
    }
}
