package com.graphqueryengine.engine.wcoj;

import com.graphqueryengine.config.DatabaseConfig;
import com.graphqueryengine.db.DatabaseManager;
import com.graphqueryengine.gremlin.GremlinExecutionResult;
import com.graphqueryengine.gremlin.GremlinTransactionalExecutionResult;
import com.graphqueryengine.gremlin.provider.GraphProvider;
import com.graphqueryengine.mapping.BackendConnectionConfig;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.MappingStore;
import com.graphqueryengine.query.api.GraphQueryTranslator;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.factory.DefaultGraphQueryTranslatorFactory;
import com.graphqueryengine.query.parser.AntlrGremlinTraversalParser;
import com.graphqueryengine.query.parser.GremlinTraversalParser;
import com.graphqueryengine.query.parser.model.GremlinParseResult;
import com.graphqueryengine.gremlin.provider.ProviderConstants;
import com.graphqueryengine.query.translate.sql.constant.GremlinToken;
import com.graphqueryengine.query.translate.sql.constant.SqlKeyword;
import com.graphqueryengine.query.translate.sql.model.HopStep;
import com.graphqueryengine.query.translate.sql.model.ParsedTraversal;
import com.graphqueryengine.query.translate.sql.model.ProjectionField;
import com.graphqueryengine.query.translate.sql.model.ProjectionKind;
import com.graphqueryengine.query.translate.sql.parse.GremlinStepParser;

import javax.script.ScriptException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongPredicate;
import java.util.logging.Logger;

/**
 * A {@link GraphProvider} that uses the Worst-Case Optimal Join (WCOJ) / Leapfrog
 * algorithm for multi-hop path traversals, dramatically outperforming SQL joins on
 * dense graphs.
 *
 * <h3>When WCOJ is used</h3>
 * <ul>
 *   <li>The parsed traversal has ≥ 1 hop (i.e. {@code out()}/{@code in()}/{@code both()}).</li>
 *   <li>The query asks only for paths, counts, or simple value projections
 *       (i.e. no GROUP BY, ORDER BY, or complex projections that require full SQL).</li>
 * </ul>
 *
 * <h3>Index storage tiers</h3>
 * <p>Adjacency indices are managed by {@link AdjacencyIndexRegistry} in two tiers:
 * <ol>
 *   <li><b>In-memory</b> ({@link AdjacencyIndex}) — edge tables with ≤ 5 M rows.
 *       O(1) HashMap lookup.</li>
 *   <li><b>Disk-backed</b> ({@link DiskBackedAdjacencyIndex}) — larger tables, stored as
 *       memory-mapped CSR files under {@code wcoj-index-cache/<mappingId>/}.
 *       The OS page cache keeps hot pages in RAM and evicts cold ones automatically.
 *       Per-mapping disk quota is enforced (default 2 GB); if exceeded the provider
 *       falls back to SQL for that query.</li>
 * </ol>
 *
 * <h3>SQL fallback</h3>
 * <p>For all other query shapes (aggregations, projections, edge queries, non-hop traversals,
 * or when a disk quota is exceeded), the provider falls back to the standard SQL translator.
 *
 * <h3>Property resolution</h3>
 * <p>After finding paths via the WCOJ index, vertex properties (e.g. {@code accountId}) are
 * resolved with a single batched SQL query:
 * {@code SELECT id, account_id FROM aml_accounts WHERE id IN (?, ?, …)}.
 * This "property fetch" is O(result-set size) rather than O(path × table-size).
 */
public class WcojGraphProvider implements GraphProvider {

    private static final Logger LOG = Logger.getLogger(WcojGraphProvider.class.getName());

    private final DatabaseManager databaseManager;
    private final MappingStore mappingStore;
    private final AdjacencyIndexRegistry indexRegistry;
    private final GraphQueryTranslator sqlTranslator;
    private final GremlinTraversalParser gremlinParser;
    private final GremlinStepParser stepParser;

    /**
     * Cache of DatabaseManager instances keyed by JDBC URL.
     * Avoids creating a new connection pool for every query when the mapping
     * declares its own backend (e.g. Trino for Iceberg mappings).
     */
    private final ConcurrentHashMap<String, DatabaseManager> managerCache = new ConcurrentHashMap<>();

    public WcojGraphProvider(DatabaseManager databaseManager, MappingStore mappingStore) {
        this(databaseManager, mappingStore, new AdjacencyIndexRegistry());
    }

    public WcojGraphProvider(DatabaseManager databaseManager, MappingStore mappingStore,
                              AdjacencyIndexRegistry indexRegistry) {
        this.databaseManager = databaseManager;
        this.mappingStore    = mappingStore;
        this.indexRegistry   = indexRegistry;
        this.sqlTranslator   = new DefaultGraphQueryTranslatorFactory().create();
        this.gremlinParser   = new AntlrGremlinTraversalParser();
        this.stepParser      = new GremlinStepParser();
    }

    @Override
    public String providerId() { return ProviderConstants.PROVIDER_WCOJ; }

    // ── Main entry point ─────────────────────────────────────────────────────

    @Override
    public GremlinExecutionResult execute(String gremlin) throws ScriptException {
        MappingConfig config = mappingStore.getActive()
                .map(MappingStore.StoredMapping::config)
                .orElse(null);
        if (config == null) return new GremlinExecutionResult(gremlin, List.of(), 0);

        // Parse the traversal to inspect its structure
        ParsedTraversal parsed;
        try {
            GremlinParseResult parseResult = gremlinParser.parse(gremlin);
            parsed = stepParser.parse(parseResult.steps(), parseResult.rootIdFilter());
        } catch (Exception ex) {
            // Parser failure → fall back to SQL
            return executeSql(gremlin, config);
        }

        // Only accelerate hop traversals that return paths/counts
        if (isWcojEligible(parsed)) {
            try {
                return executeWcoj(gremlin, parsed, config);
            } catch (WcojFallbackException ex) {
                LOG.info("[WCOJ] Falling back to SQL: " + ex.getMessage());
            } catch (Exception ex) {
                LOG.warning("[WCOJ] Unexpected error, falling back to SQL: " + ex.getMessage());
            }
        }

        return executeSql(gremlin, config);
    }

    @Override
    public GremlinTransactionalExecutionResult executeInTransaction(String gremlin)
            throws ScriptException {
        GremlinExecutionResult r = execute(gremlin);
        return new GremlinTransactionalExecutionResult(
                r.gremlin(), r.results(), r.resultCount(),
                ProviderConstants.TX_MODE_READ_ONLY, ProviderConstants.TX_STATUS_COMMITTED);
    }

    // ── WCOJ execution path ──────────────────────────────────────────────────

    private GremlinExecutionResult executeWcoj(String gremlin,
                                               ParsedTraversal parsed,
                                               MappingConfig config)
            throws ScriptException, WcojFallbackException {

        List<HopStep> hops = parsed.hops();
        int hopCount = hops.size();

        // Resolve the active mapping ID for per-mapping index isolation and quota
        String mappingId = mappingStore.getActive()
                .map(MappingStore.StoredMapping::id)
                .orElse("default");

        try (Connection conn = connectionFor(config).connection()) {

            // 1. Load (or reuse) adjacency indices for every hop
            NeighbourLookup[] indices = new NeighbourLookup[hopCount];
            boolean[] outDirs         = new boolean[hopCount];
            for (int i = 0; i < hopCount; i++) {
                HopStep hop = hops.get(i);
                String label = hop.singleLabel();
                NeighbourLookup idx = indexRegistry.getOrLoad(conn, config, label, mappingId);
                if (idx == null) {
                    throw new WcojFallbackException(
                            "Edge table for '" + label + "' exceeds disk quota for mapping '"
                            + mappingId + "'");
                }
                indices[i] = idx;
                outDirs[i] = GremlinToken.OUT.equals(hop.direction())
                        || GremlinToken.BOTH.equals(hop.direction());
            }

            // 2. Build the start-vertex filter from parsed.filters()
            LongPredicate startFilter = buildStartFilter(conn, config, parsed);

            // 3. Run the leapfrog join
            int limit = parsed.limit() != null ? parsed.limit() : 0;
            if (parsed.preHopLimit() != null && limit <= 0)
                limit = parsed.preHopLimit();

            LeapfrogTrieJoin join = new LeapfrogTrieJoin(
                    indices, outDirs, parsed.simplePathRequested(), startFilter, limit);

            long t0 = System.currentTimeMillis();
            List<long[]> paths = join.enumerate();
            long ms = System.currentTimeMillis() - t0;
            LOG.info(String.format("[WCOJ] %d-hop join produced %d paths in %d ms",
                    hopCount, paths.size(), ms));

            // 4. count() shortcut
            if (parsed.countRequested()) {
                long cnt = parsed.dedupRequested()
                        ? paths.stream().mapToLong(p -> p[hopCount]).distinct().count()
                        : paths.size();
                return new GremlinExecutionResult(gremlin, List.of(cnt), 1);
            }

            // 5. Resolve vertex properties for path output
            return buildPathResult(gremlin, paths, hopCount, parsed, config, conn);

        } catch (SQLException ex) {
            throw new ScriptException("WCOJ SQL error: " + ex.getMessage());
        }
    }

    // ── Result building ──────────────────────────────────────────────────────

    private GremlinExecutionResult buildPathResult(String gremlin,
                                                   List<long[]> paths,
                                                   int hopCount,
                                                   ParsedTraversal parsed,
                                                   MappingConfig config,
                                                   Connection conn) throws SQLException {
        List<String> byProps = parsed.pathByProperties();

        // ── Simple-property projections ────────────────────────────────────────
        // project('p1','p2',...).by('p1').by('p2') with all EDGE_PROPERTY kinds:
        // resolve terminal vertex properties and return scalar (single column) or
        // Map (multiple columns) — same semantics as the SQL provider.
        if (!parsed.projections().isEmpty() && isSimplePropertyProjection(parsed.projections())) {
            String terminalLabel = resolveTerminalLabel(parsed, config, parsed.label(), hopCount);
            Set<Long> terminalIds = new LinkedHashSet<>();
            for (long[] path : paths) terminalIds.add(path[hopCount]);

            List<ProjectionField> projFields = parsed.projections();
            boolean single = projFields.size() == 1;

            if (single) {
                // Single projection → return scalar values (mirrors SQL provider scalar path)
                String prop = projFields.get(0).property();
                Map<Long, Object> propMap = fetchProperty(conn, config, terminalLabel, prop, terminalIds);
                List<Object> results = new ArrayList<>();
                for (long[] path : paths) results.add(propMap.getOrDefault(path[hopCount], path[hopCount]));
                return new GremlinExecutionResult(gremlin, results, results.size());
            } else {
                // Multiple projections → return Map per row (List-wrapped values like SQL provider)
                Map<String, Map<Long, Object>> propMaps = new LinkedHashMap<>();
                for (ProjectionField pf : projFields) {
                    propMaps.put(pf.alias(),
                            fetchProperty(conn, config, terminalLabel, pf.property(), terminalIds));
                }
                List<Object> results = new ArrayList<>();
                for (long[] path : paths) {
                    long termId = path[hopCount];
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (ProjectionField pf : projFields) {
                        Object val = propMaps.get(pf.alias()).getOrDefault(termId, termId);
                        row.put(pf.alias(), List.of(val));
                    }
                    results.add(row);
                }
                return new GremlinExecutionResult(gremlin, results, results.size());
            }
        }

        if (!parsed.pathSeen() || byProps.isEmpty()) {
            // If a values('property') step was requested, resolve the terminal vertex property
            String valuesProp = parsed.valueProperty();
            if (valuesProp != null && !valuesProp.isBlank()) {
                String terminalLabel = resolveTerminalLabel(parsed, config, parsed.label(), hopCount);
                Set<Long> terminalIds = new LinkedHashSet<>();
                for (long[] path : paths) terminalIds.add(path[hopCount]);
                Map<Long, Object> propMap = fetchProperty(conn, config, terminalLabel, valuesProp, terminalIds);
                List<Object> results = new ArrayList<>();
                for (long[] path : paths) results.add(propMap.getOrDefault(path[hopCount], path[hopCount]));
                return new GremlinExecutionResult(gremlin, results, results.size());
            }
            // No property requested — return raw terminal vertex IDs
            List<Object> results = new ArrayList<>();
            for (long[] path : paths) results.add(path[hopCount]);
            return new GremlinExecutionResult(gremlin, results, results.size());
        }

        // Determine which vertex mapping corresponds to each hop position
        // Position 0 = start label, positions 1..hopCount = inferred from edge mapping
        String startLabel = parsed.label();
        List<String> vertexLabels = resolvePathVertexLabels(parsed, config, startLabel, hopCount);

        // Collect all IDs per position for batched property fetch
        Map<Integer, Set<Long>> idsByPos = new HashMap<>();
        for (int pos = 0; pos <= hopCount; pos++) idsByPos.put(pos, new LinkedHashSet<>());
        for (long[] path : paths) {
            for (int pos = 0; pos <= hopCount; pos++) {
                idsByPos.get(pos).add(path[pos]);
            }
        }

        // Batch-fetch properties for each hop position.
        // TinkerPop semantics: if a single by() modulator is given it applies to all positions.
        Map<Integer, Map<Long, Object>> propByPos = new HashMap<>();
        for (int pos = 0; pos <= hopCount; pos++) {
            String propName;
            if (byProps.isEmpty()) {
                propName = null;
            } else if (byProps.size() == 1) {
                // single by() modulator — apply to all positions (TinkerPop cycling semantics)
                propName = byProps.get(0);
            } else {
                propName = pos < byProps.size() ? byProps.get(pos) : null;
            }
            if (propName == null) continue;
            String label = pos < vertexLabels.size() ? vertexLabels.get(pos) : startLabel;
            propByPos.put(pos, fetchProperty(conn, config, label, propName,
                    idsByPos.get(pos)));
        }

        // Build result rows
        List<Object> results = new ArrayList<>();
        for (long[] path : paths) {
            List<Object> row = new ArrayList<>();
            for (int pos = 0; pos <= hopCount; pos++) {
                Map<Long, Object> props = propByPos.get(pos);
                row.add(props != null ? props.getOrDefault(path[pos], path[pos]) : path[pos]);
            }
            results.add(row);
        }
        return new GremlinExecutionResult(gremlin, results, results.size());
    }

    /**
     * Fetches a single property column for a set of vertex IDs using a batched IN query.
     * O(|ids|) rather than O(|table|).
     */
    private Map<Long, Object> fetchProperty(Connection conn, MappingConfig config,
                                            String vertexLabel, String propName,
                                            Set<Long> ids) throws SQLException {
        if (ids.isEmpty()) return Collections.emptyMap();
        var vm = config.vertices().get(vertexLabel);
        if (vm == null) return Collections.emptyMap();
        String col = vm.properties().get(propName);
        if (col == null) return Collections.emptyMap();

        String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
        String sql = SqlKeyword.SELECT + vm.idColumn() + ", " + col
                + SqlKeyword.FROM + vm.table() + SqlKeyword.WHERE + vm.idColumn()
                + " IN (" + placeholders + ")";

        Map<Long, Object> result = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int i = 1;
            for (long id : ids) ps.setLong(i++, id);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) result.put(rs.getLong(1), rs.getObject(2));
            }
        }
        return result;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Returns {@code true} when the WCOJ engine can accelerate this traversal.
     *
     * <p>In addition to pure path/count/values queries, WCOJ can now also handle
     * <em>simple-property projections</em> — {@code project('p').by('p')} patterns
     * where every {@code by()} modulator is a plain {@link ProjectionKind#EDGE_PROPERTY}
     * reference with no sub-traversal (degree counts, choose(), etc.).
     * Such projections are resolved with a single batched SQL property-fetch after the
     * leapfrog join, just like {@code values('p')}.
     */
    private static boolean isWcojEligible(ParsedTraversal parsed) {
        if (parsed.hops().isEmpty()) return false;
        // Only handle path/count/values queries; defer aggregations to SQL
        if (parsed.sumRequested() || parsed.meanRequested()) return false;
        if (parsed.groupCountProperty() != null || parsed.groupCountKeySpec() != null) return false;
        if (!parsed.selectFields().isEmpty()) return false;
        if (parsed.orderByProperty() != null) return false;
        // Allow projections only when every by() is a simple property reference
        if (!parsed.projections().isEmpty() && !isSimplePropertyProjection(parsed.projections())) {
            return false;
        }
        // Edge-step hops (outE/inE/bothE) and bidirectional (both) are not yet supported in WCOJ
        for (HopStep hop : parsed.hops()) {
            if (GremlinToken.OUT_E.equals(hop.direction())
                    || GremlinToken.IN_E.equals(hop.direction())
                    || GremlinToken.BOTH_E.equals(hop.direction())
                    || GremlinToken.BOTH.equals(hop.direction())) {
                return false;
            }
            // Multi-label hops are fine — handled by union in the join
            if (hop.labels().size() > 1) return false; // TODO: support multi-label
        }
        return true;
    }

    /**
     * Returns {@code true} if all projection fields are simple vertex/edge property
     * references (no sub-traversal degree counts, choose(), etc.).
     * These can be resolved after the WCOJ join with a batched SQL property fetch.
     */
    private static boolean isSimplePropertyProjection(List<ProjectionField> projections) {
        for (ProjectionField pf : projections) {
            if (pf.kind() != ProjectionKind.EDGE_PROPERTY
                    && pf.kind() != ProjectionKind.IDENTITY) {
                return false;
            }
        }
        return true;
    }

    /**
     * Builds a predicate on start vertex IDs from the parsed {@code has()} filters.
     * Falls back to a table scan if any filter references a property not in the adjacency index.
     * Returns {@code null} if no filter (accept all vertices).
     */
    private LongPredicate buildStartFilter(Connection conn, MappingConfig config,
                                           ParsedTraversal parsed)
            throws SQLException, WcojFallbackException {
        if (parsed.filters().isEmpty() && parsed.whereClause() == null
                && parsed.preHopLimit() == null) {
            return null;  // no filter — iterate all source vertices
        }

        // Resolve the start vertex set via SQL (small result expected due to LIMIT)
        String label = parsed.label();
        var vm = config.vertices().get(label);
        if (vm == null) throw new WcojFallbackException("Unknown vertex label: " + label);

        // Build a simple SELECT id FROM <table> WHERE ... LIMIT n to get seed IDs
        TranslationResult seedQuery = sqlTranslator.translate(
                buildSeedGremlin(parsed), config);
        vm.table();

        // Use the translated SQL's WHERE + LIMIT to get seed IDs
        // Re-derive: use the SQL translator output but strip SELECT ... to just get IDs
        String seedSql = SqlKeyword.SELECT + vm.idColumn() + SqlKeyword.FROM + "(" + seedQuery.sql() + ") _seed";

        Set<Long> seedIds = new LinkedHashSet<>();
        try (PreparedStatement ps = conn.prepareStatement(seedSql)) {
            List<Object> params = seedQuery.parameters();
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) seedIds.add(rs.getLong(1));
            }
        } catch (SQLException ex) {
            // If derived-table approach fails, fall back to full SQL
            throw new WcojFallbackException("Seed query failed: " + ex.getMessage());
        }

        if (seedIds.isEmpty()) return id -> false;
        return seedIds::contains;
    }

    /** Build a Gremlin string that returns just the start vertices (no hops). */
    private static String buildSeedGremlin(ParsedTraversal parsed) {
        // Reconstruct a Gremlin-like handle — use the translator by stripping hops.
        // We represent the traversal up to (not including) the first hop.
        // The simplest approach: reconstruct from the label + filters.
        StringBuilder sb = new StringBuilder("g.V().hasLabel('")
                .append(parsed.label()).append("')");
        for (var f : parsed.filters()) {
            sb.append(".has('").append(f.property()).append("','").append(f.value()).append("')");
        }
        if (parsed.preHopLimit() != null) sb.append(".limit(").append(parsed.preHopLimit()).append(")");
        return sb.toString();
    }

    /** Walk the hop chain to determine vertex label at each path position. */
    private static List<String> resolvePathVertexLabels(ParsedTraversal parsed,
                                                        MappingConfig config,
                                                        String startLabel,
                                                        int hopCount) {
        List<String> labels = new ArrayList<>();
        labels.add(startLabel);
        String current = startLabel;
        for (int i = 0; i < hopCount; i++) {
            HopStep hop = parsed.hops().get(i);
            String edgeLabel = hop.singleLabel();
            var em = config.edges().get(edgeLabel);
            if (em == null) { labels.add(current); continue; }
            boolean out = GremlinToken.OUT.equals(hop.direction());
            String next = out ? em.inVertexLabel() : em.outVertexLabel();
            if (next == null) next = current;  // same-type edge (e.g. TRANSFER Account→Account)
            labels.add(next);
            current = next;
        }
        return labels;
    }

    /** Resolves the vertex label at the terminal (last) hop position. */
    private static String resolveTerminalLabel(ParsedTraversal parsed, MappingConfig config,
                                               String startLabel, int hopCount) {
        String current = startLabel;
        for (int i = 0; i < hopCount; i++) {
            HopStep hop = parsed.hops().get(i);
            if (hop.labels().isEmpty()) continue;
            var em = config.edges().get(hop.singleLabel());
            if (em == null) continue;
            boolean out = GremlinToken.OUT.equals(hop.direction());
            String next = out ? em.inVertexLabel() : em.outVertexLabel();
            if (next != null) current = next;
        }
        return current;
    }

    // ── SQL fallback ─────────────────────────────────────────────────────────

    private GremlinExecutionResult executeSql(String gremlin, MappingConfig config)
            throws ScriptException {
        TranslationResult tr;
        try {
            tr = sqlTranslator.translate(gremlin, config);
        } catch (IllegalArgumentException ex) {
            throw new ScriptException("SQL translation failed: " + ex.getMessage());
        }

        try (Connection conn = connectionFor(config).connection();
             PreparedStatement ps = conn.prepareStatement(tr.sql())) {

            List<Object> params = tr.parameters();
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));

            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                int cols = meta.getColumnCount();
                boolean isPath = cols > 1;
                for (int c = 1; c <= cols && isPath; c++) {
                    if (!meta.getColumnLabel(c).matches(".+\\d+$")) isPath = false;
                }
                List<Object> results = new ArrayList<>();
                while (rs.next()) {
                    if (cols == 1) {
                        results.add(rs.getObject(1));
                    } else if (isPath) {
                        List<Object> row = new ArrayList<>();
                        for (int c = 1; c <= cols; c++) row.add(rs.getObject(c));
                        results.add(row);
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

    /**
     * Returns the {@link DatabaseManager} appropriate for {@code config}.
     *
     * <p>If the mapping declares its own backends (e.g. a Trino/Iceberg URL), the
     * first backend's connection config is used.  This ensures WCOJ uses Trino when
     * the active mapping points at Iceberg, rather than always falling back to the
     * default H2 {@code DB_URL} environment variable.
     *
     * <p>Managers are cached by JDBC URL so the connection pool is reused across queries.
     */
    private DatabaseManager connectionFor(MappingConfig config) {
        if (config != null && config.backends() != null && !config.backends().isEmpty()) {
            BackendConnectionConfig bcc = config.backends().values().iterator().next();
            if (bcc.url() != null && !bcc.url().isBlank()) {
                return managerCache.computeIfAbsent(bcc.url(), url -> {
                    LOG.info("[WCOJ] Using mapping backend connection: " + url);
                    return new DatabaseManager(
                            new DatabaseConfig(bcc.url(), bcc.user(), bcc.password(), bcc.driverClass()));
                });
            }
        }
        return databaseManager;  // fall back to default DB_URL
    }

    /** Signals that the WCOJ path is not viable for this query; caller should use SQL. */
    static final class WcojFallbackException extends Exception {
        WcojFallbackException(String msg) { super(msg); }
    }
}

