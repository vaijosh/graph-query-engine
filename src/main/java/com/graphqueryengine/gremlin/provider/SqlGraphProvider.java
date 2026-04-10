package com.graphqueryengine.gremlin.provider;

import com.graphqueryengine.db.DatabaseManager;
import com.graphqueryengine.gremlin.GremlinExecutionResult;
import com.graphqueryengine.gremlin.GremlinTransactionalExecutionResult;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.MappingStore;
import com.graphqueryengine.query.api.GraphQueryTranslator;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.factory.DefaultGraphQueryTranslatorFactory;

import javax.script.ScriptException;
import java.sql.*;
import java.util.*;

/**
 * A {@link GraphProvider} that executes Gremlin queries by translating them to SQL
 * and running them directly against a JDBC database (e.g. H2, Trino / Iceberg).
 *
 * <h3>Single-backend mode</h3>
 * <p>Constructed with a single {@link DatabaseManager} — all queries go to that one connection.
 * Activated via {@code GRAPH_PROVIDER=sql} when only one backend is configured.
 *
 * <h3>Multi-backend / hybrid mode</h3>
 * <p>Constructed with a {@link BackendRegistry} — the connection is chosen per query based
 * on the {@code datasource} field of the active mapping's vertex or edge entry
 * (falling back to {@link MappingConfig#defaultDatasource()}, then to the registry default).
 * This allows a single Gremlin query to reference tables that live in different backends.
 */
public class SqlGraphProvider implements GraphProvider {

    private final DatabaseManager databaseManager;   // null when registry is set
    private final BackendRegistry backendRegistry;   // null in single-backend mode
    private final GraphQueryTranslator translator;
    private final MappingStore mappingStore;

    // ── Constructors ─────────────────────────────────────────────────────────

    /** Single-backend constructor (legacy / default). */
    public SqlGraphProvider(DatabaseManager databaseManager, MappingStore mappingStore) {
        this.databaseManager  = databaseManager;
        this.backendRegistry  = null;
        this.mappingStore     = mappingStore;
        this.translator       = new DefaultGraphQueryTranslatorFactory().create();
    }

    /** Multi-backend constructor: connection is resolved per-query from the registry. */
    public SqlGraphProvider(BackendRegistry backendRegistry, MappingStore mappingStore) {
        this.databaseManager  = null;
        this.backendRegistry  = backendRegistry;
        this.mappingStore     = mappingStore;
        this.translator       = new DefaultGraphQueryTranslatorFactory().create();
    }

    // ── Public accessors ─────────────────────────────────────────────────────

    /** Returns the single {@link DatabaseManager}, or {@code null} in multi-backend mode. */
    public DatabaseManager databaseManager() {
        return databaseManager;
    }

    @Override
    public String providerId() {
        return backendRegistry != null ? "sql-multi" : "sql";
    }

    // ── Execution ────────────────────────────────────────────────────────────

    // sql is always produced by SqlTranslator from a parsed Gremlin AST, never from
    // raw user input. All user-supplied values travel via the `params` list and are
    // bound with ps.setObject() — no SQL injection risk. Suppress IDE dataflow warning.
    @Override
    @SuppressWarnings("SqlSourceToSinkFlow")
    public GremlinExecutionResult execute(String gremlin) throws ScriptException {
        MappingConfig mappingConfig = mappingStore.getActive()
                .map(MappingStore.StoredMapping::config)
                .orElse(null);

        if (mappingConfig == null) {
            return new GremlinExecutionResult(gremlin, List.of(), 0);
        }

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
        try (Connection conn = dm.connection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

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
        } catch (SQLException ex) {
            throw new ScriptException("SQL execution error: " + ex.getMessage());
        }
    }

    @Override
    public GremlinTransactionalExecutionResult executeInTransaction(String gremlin) throws ScriptException {
        GremlinExecutionResult result = execute(gremlin);
        return new GremlinTransactionalExecutionResult(
                result.gremlin(), result.results(), result.resultCount(),
                "read-only", "committed");
    }

    // ── Private helpers ──────────────────────────────────────────────────────

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
