package com.graphqueryengine.engine.wcoj;

import com.graphqueryengine.db.DatabaseManager;
import com.graphqueryengine.gremlin.GremlinExecutionResult;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.MappingStore;
import com.graphqueryengine.query.api.GraphQueryTranslator;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.factory.DefaultGraphQueryTranslatorFactory;
import com.graphqueryengine.query.parser.AntlrGremlinTraversalParser;
import com.graphqueryengine.query.parser.GremlinTraversalParser;
import com.graphqueryengine.query.parser.model.GremlinParseResult;
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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.logging.Logger;

/**
 * WCOJ (Worst-Case Optimal Join / Leapfrog Trie Join) optimiser for multi-hop
 * path traversals.
 *
 * This class encapsulates the WCOJ acceleration logic previously embedded in the
 * (now-removed) {@code WcojGraphProvider}.  It is now an <em>optional optimiser</em> called by
 * {@link com.graphqueryengine.gremlin.provider.SqlGraphProvider} before falling
 * back to SQL.  Enable/disable via the {@code WCOJ_ENABLED} environment variable
 * (default: {@code true}).
 *
 * <h3>When WCOJ is used</h3>
 * <ul>
 *   <li>The parsed traversal has ≥ 1 hop ({@code out()}/{@code in()}/{@code both()}).</li>
 *   <li>The query asks only for paths, counts, or simple value projections
 *       (no GROUP BY, ORDER BY, or complex projections that require full SQL).</li>
 * </ul>
 *
 * <p>For all other shapes the optimiser returns {@code null}, and the caller falls
 * back to the standard SQL translator.
 */
public class WcojOptimiser {

    private static final Logger LOG = Logger.getLogger(WcojOptimiser.class.getName());

    private final MappingStore mappingStore;
    private final AdjacencyIndexRegistry indexRegistry;
    private final GremlinTraversalParser gremlinParser;
    private final GremlinStepParser stepParser;
    private final GraphQueryTranslator sqlTranslator;

    public WcojOptimiser(MappingStore mappingStore, AdjacencyIndexRegistry indexRegistry) {
        this.mappingStore  = mappingStore;
        this.indexRegistry = indexRegistry;
        this.gremlinParser = new AntlrGremlinTraversalParser();
        this.stepParser    = new GremlinStepParser();
        this.sqlTranslator = new DefaultGraphQueryTranslatorFactory().create();
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Value-object returned on a successful WCOJ execution.
     * {@code null} is returned when the query is not WCOJ-eligible.
     */
    public record Result(GremlinExecutionResult executionResult) {}

    /**
     * Attempts to execute {@code gremlin} using the Leapfrog Trie Join.
     *
     * @return A {@link Result} wrapping the execution result, or {@code null} if the
     *         query is not eligible for WCOJ (caller should use SQL instead).
     * @throws ScriptException if WCOJ was eligible but execution failed.
     */
    public Result tryExecute(String gremlin, MappingConfig config,
                             DatabaseManager databaseManager) throws ScriptException {
        // 1. Parse
        ParsedTraversal parsed;
        try {
            GremlinParseResult pr = gremlinParser.parse(gremlin);
            parsed = stepParser.parse(pr.steps(), pr.rootIdFilter());
        } catch (Exception ex) {
            return null; // parse failure → caller uses SQL
        }

        // 2. Eligibility check
        if (!isWcojEligible(parsed)) {
            return null;
        }

        // 3. Execute via Leapfrog
        String mappingId = mappingStore.getActive()
                .map(MappingStore.StoredMapping::id)
                .orElse("default");
        try {
            GremlinExecutionResult result = executeWcoj(gremlin, parsed, config,
                    mappingId, databaseManager);
            return new Result(result);
        } catch (WcojFallbackException ex) {
            LOG.info("[WCOJ] Falling back to SQL: " + ex.getMessage());
            return null;
        } catch (Exception ex) {
            LOG.warning("[WCOJ] Unexpected error, falling back to SQL: " + ex.getMessage());
            return null;
        }
    }

    // ── WCOJ execution ────────────────────────────────────────────────────────

    private GremlinExecutionResult executeWcoj(String gremlin,
                                               ParsedTraversal parsed,
                                               MappingConfig config,
                                               String mappingId,
                                               DatabaseManager databaseManager)
            throws ScriptException, WcojFallbackException {

        List<HopStep> hops = parsed.hops();
        int hopCount = hops.size();

        try (Connection conn = databaseManager.connection()) {

            // 1. Load adjacency indices for every hop
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

            // 2. Build start-vertex filter
            LongPredicate startFilter = buildStartFilter(conn, config, parsed);

            // 3. Run leapfrog join
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

            // 5. Build property result
            return buildPathResult(gremlin, paths, hopCount, parsed, config, conn);

        } catch (SQLException ex) {
            throw new ScriptException("WCOJ SQL error: " + ex.getMessage());
        }
    }

    // ── Result building ───────────────────────────────────────────────────────

    private GremlinExecutionResult buildPathResult(String gremlin,
                                                   List<long[]> paths,
                                                   int hopCount,
                                                   ParsedTraversal parsed,
                                                   MappingConfig config,
                                                   Connection conn) throws SQLException {
        List<String> byProps = parsed.pathByProperties();

        // Simple-property projections (project('p1','p2',...).by(...))
        if (!parsed.projections().isEmpty() && isSimplePropertyProjection(parsed.projections())) {
            String terminalLabel = resolveTerminalLabel(parsed, config, parsed.label(), hopCount);
            Set<Long> terminalIds = new LinkedHashSet<>();
            for (long[] path : paths) terminalIds.add(path[hopCount]);

            List<ProjectionField> projFields = parsed.projections();
            boolean single = projFields.size() == 1;

            if (single) {
                String prop = projFields.get(0).property();
                Map<Long, Object> propMap = fetchProperty(conn, config, terminalLabel, prop, terminalIds);
                List<Object> results = new ArrayList<>();
                for (long[] path : paths) results.add(propMap.getOrDefault(path[hopCount], path[hopCount]));
                return new GremlinExecutionResult(gremlin, results, results.size());
            } else {
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
            List<Object> results = new ArrayList<>();
            for (long[] path : paths) results.add(path[hopCount]);
            return new GremlinExecutionResult(gremlin, results, results.size());
        }

        String startLabel = parsed.label();
        List<String> vertexLabels = resolvePathVertexLabels(parsed, config, startLabel, hopCount);

        Map<Integer, Set<Long>> idsByPos = new HashMap<>();
        for (int pos = 0; pos <= hopCount; pos++) idsByPos.put(pos, new LinkedHashSet<>());
        for (long[] path : paths) {
            for (int pos = 0; pos <= hopCount; pos++) idsByPos.get(pos).add(path[pos]);
        }

        Map<Integer, Map<Long, Object>> propByPos = new HashMap<>();
        for (int pos = 0; pos <= hopCount; pos++) {
            String propName;
            if (byProps.isEmpty()) {
                propName = null;
            } else if (byProps.size() == 1) {
                propName = byProps.get(0);
            } else {
                propName = pos < byProps.size() ? byProps.get(pos) : null;
            }
            if (propName == null) continue;
            String label = pos < vertexLabels.size() ? vertexLabels.get(pos) : startLabel;
            propByPos.put(pos, fetchProperty(conn, config, label, propName, idsByPos.get(pos)));
        }

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

    // ── Property fetch ────────────────────────────────────────────────────────

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

    // ── Eligibility ───────────────────────────────────────────────────────────

    /**
     * Returns {@code true} when the WCOJ engine can accelerate this traversal.
     */
    private static boolean isWcojEligible(ParsedTraversal parsed) {
        if (parsed.hops().isEmpty()) return false;
        if (parsed.sumRequested() || parsed.meanRequested()) return false;
        if (parsed.groupCountProperty() != null || parsed.groupCountKeySpec() != null) return false;
        if (!parsed.selectFields().isEmpty()) return false;
        if (parsed.orderByProperty() != null) return false;
        if (!parsed.projections().isEmpty() && !isSimplePropertyProjection(parsed.projections())) {
            return false;
        }
        for (HopStep hop : parsed.hops()) {
            if (GremlinToken.OUT_E.equals(hop.direction())
                    || GremlinToken.IN_E.equals(hop.direction())
                    || GremlinToken.BOTH_E.equals(hop.direction())
                    || GremlinToken.BOTH.equals(hop.direction())) {
                return false;
            }
            if (hop.labels().size() > 1) return false;
        }
        return true;
    }

    private static boolean isSimplePropertyProjection(List<ProjectionField> projections) {
        for (ProjectionField pf : projections) {
            if (pf.kind() != ProjectionKind.EDGE_PROPERTY && pf.kind() != ProjectionKind.IDENTITY) {
                return false;
            }
        }
        return true;
    }

    // ── Start-filter ──────────────────────────────────────────────────────────

    private LongPredicate buildStartFilter(Connection conn, MappingConfig config,
                                           ParsedTraversal parsed)
            throws SQLException, WcojFallbackException {
        if (parsed.filters().isEmpty() && parsed.whereClause() == null
                && parsed.preHopLimit() == null) {
            return null;
        }

        String label = parsed.label();
        var vm = config.vertices().get(label);
        if (vm == null) throw new WcojFallbackException("Unknown vertex label: " + label);

        TranslationResult seedQuery = sqlTranslator.translate(buildSeedGremlin(parsed), config);
        String seedSql = SqlKeyword.SELECT + vm.idColumn() + SqlKeyword.FROM
                + "(" + seedQuery.sql() + ") _seed";

        Set<Long> seedIds = new LinkedHashSet<>();
        try (PreparedStatement ps = conn.prepareStatement(seedSql)) {
            List<Object> params = seedQuery.parameters();
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) seedIds.add(rs.getLong(1));
            }
        } catch (SQLException ex) {
            throw new WcojFallbackException("Seed query failed: " + ex.getMessage());
        }

        if (seedIds.isEmpty()) return id -> false;
        return seedIds::contains;
    }

    private static String buildSeedGremlin(ParsedTraversal parsed) {
        StringBuilder sb = new StringBuilder("g.V().hasLabel('")
                .append(parsed.label()).append("')");
        for (var f : parsed.filters()) {
            sb.append(".has('").append(f.property()).append("','").append(f.value()).append("')");
        }
        if (parsed.preHopLimit() != null) sb.append(".limit(").append(parsed.preHopLimit()).append(")");
        return sb.toString();
    }

    // ── Label resolution ──────────────────────────────────────────────────────

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
            if (next == null) next = current;
            labels.add(next);
            current = next;
        }
        return labels;
    }

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

    // ── Internal exception ────────────────────────────────────────────────────

    /** Signals that WCOJ is not viable for this query; caller should fall back to SQL. */
    static final class WcojFallbackException extends Exception {
        WcojFallbackException(String msg) { super(msg); }
    }
}

