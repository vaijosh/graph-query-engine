package com.graphqueryengine.query.translate.sql;

import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.query.api.QueryPlan;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.parser.LegacyGremlinTraversalParser;
import com.graphqueryengine.query.parser.model.GremlinParseResult;
import com.graphqueryengine.query.translate.sql.dialect.SqlDialect;
import com.graphqueryengine.query.translate.sql.dialect.StandardSqlDialect;
import com.graphqueryengine.query.translate.sql.mapping.SqlMappingResolver;
import com.graphqueryengine.query.translate.sql.model.ParsedTraversal;
import com.graphqueryengine.query.translate.sql.parse.GremlinStepParser;
import com.graphqueryengine.query.translate.sql.plan.QueryPlanBuilder;
import com.graphqueryengine.query.translate.sql.render.EdgeSqlBuilder;
import com.graphqueryengine.query.translate.sql.render.HopSqlBuilder;
import com.graphqueryengine.query.translate.sql.render.SqlRenderHelper;
import com.graphqueryengine.query.translate.sql.render.VertexSqlBuilder;
import com.graphqueryengine.query.translate.sql.where.WhereClauseBuilder;

/**
 * Facade that translates Gremlin traversal strings (or pre-parsed
 * {@link GremlinParseResult} objects) into SQL.
 *
 * <h2>Design</h2>
 * <p>This class is intentionally thin — it wires together a set of focused,
 * single-responsibility collaborators and delegates every non-trivial concern to them:
 *
 * <ul>
 *   <li>{@link GremlinStepParser} — Gremlin step → {@link ParsedTraversal} model</li>
 *   <li>{@link SqlMappingResolver} — label/property → table/column resolution</li>
 *   <li>{@link WhereClauseBuilder} — WHERE fragment generation</li>
 *   <li>{@link SqlRenderHelper} — shared SQL utilities (LIMIT, ORDER BY, literals…)</li>
 *   <li>{@link VertexSqlBuilder} — {@code g.V()} SQL generation (no hops)</li>
 *   <li>{@link HopSqlBuilder} — multi-hop traversal SQL generation</li>
 *   <li>{@link EdgeSqlBuilder} — {@code g.E()} SQL generation</li>
 *   <li>{@link QueryPlanBuilder} — {@link QueryPlan} construction without SQL output</li>
 * </ul>
 *
 * <p>The public API (constructors + {@code translate} / {@code translateWithPlan} /
 * {@code plan} / {@code planFromParsed} methods) is fully preserved.
 */
public class GremlinSqlTranslator {

    private final SqlDialect dialect;

    // ── Collaborators ─────────────────────────────────────────────────────────
    private final GremlinStepParser stepParser;

    // Render builders are created lazily per mapping config (stateless wrt config);
    // they are stored here as they are stateless and re-entrant.
    // (MappingConfig arrives at call time, so builders that need it receive it as an argument.)

    // ── Constructors ──────────────────────────────────────────────────────────

    /** Constructs a translator using the default {@link StandardSqlDialect}. */
    public GremlinSqlTranslator() {
        this(new StandardSqlDialect());
    }

    /** Constructs a translator using the given SQL dialect. */
    public GremlinSqlTranslator(SqlDialect dialect) {
        this.dialect    = dialect;
        this.stepParser = new GremlinStepParser();
    }

    // ── Public API ────────────────────────────────────────────────────────────

    public TranslationResult translate(String gremlin, MappingConfig mappingConfig) {
        GremlinParseResult parsed = new LegacyGremlinTraversalParser().parse(gremlin);
        return translate(parsed, mappingConfig);
    }

    public TranslationResult translate(GremlinParseResult parsed, MappingConfig mappingConfig) {
        if (parsed == null) throw new IllegalArgumentException("Parsed Gremlin query is required");
        ParsedTraversal traversal = stepParser.parse(parsed.steps(), parsed.rootIdFilter());
        return buildSql(parsed.vertexQuery(), traversal, mappingConfig);
    }

    /**
     * Translates a Gremlin query string to SQL and attaches a {@link QueryPlan}
     * capturing every mapping decision made during translation.
     */
    public TranslationResult translateWithPlan(String gremlin, MappingConfig mappingConfig) {
        GremlinParseResult parsed = new LegacyGremlinTraversalParser().parse(gremlin);
        return translateWithPlan(parsed, mappingConfig);
    }

    /** Like {@link #translate} but also attaches a {@link QueryPlan} to the result. */
    public TranslationResult translateWithPlan(GremlinParseResult parsed, MappingConfig mappingConfig) {
        if (parsed == null) throw new IllegalArgumentException("Parsed Gremlin query is required");
        ParsedTraversal traversal = stepParser.parse(parsed.steps(), parsed.rootIdFilter());
        TranslationResult base    = buildSql(parsed.vertexQuery(), traversal, mappingConfig);
        QueryPlan qPlan           = makePlanBuilder(mappingConfig).build(parsed.vertexQuery(), traversal, mappingConfig);
        return new TranslationResult(base.sql(), base.parameters(), qPlan);
    }

    /** Builds a {@link QueryPlan} without producing SQL. */
    public QueryPlan plan(String gremlin, MappingConfig mappingConfig) {
        GremlinParseResult parsed = new LegacyGremlinTraversalParser().parse(gremlin);
        return planFromParsed(parsed, mappingConfig);
    }

    /** Builds a {@link QueryPlan} from an already-parsed result without producing SQL. */
    public QueryPlan planFromParsed(GremlinParseResult parsed, MappingConfig mappingConfig) {
        if (parsed == null) throw new IllegalArgumentException("Parsed Gremlin query is required");
        ParsedTraversal traversal = stepParser.parse(parsed.steps(), parsed.rootIdFilter());
        return makePlanBuilder(mappingConfig).build(parsed.vertexQuery(), traversal, mappingConfig);
    }

    // ── Internal wiring ───────────────────────────────────────────────────────

    private TranslationResult buildSql(boolean vertexQuery, ParsedTraversal traversal,
                                        MappingConfig mappingConfig) {
        SqlMappingResolver resolver = new SqlMappingResolver(mappingConfig);
        WhereClauseBuilder where    = new WhereClauseBuilder(resolver);
        SqlRenderHelper    helper   = new SqlRenderHelper(dialect, resolver, stepParser);

        if (vertexQuery) {
            // groupCount+hops (alias-keyed groupCount) is handled by VertexSqlBuilder, not HopSqlBuilder
            boolean hasAggregation = traversal.groupCountProperty() != null || traversal.groupCountKeySpec() != null;
            if (!traversal.hops().isEmpty() && !hasAggregation) {
                return new HopSqlBuilder(resolver, where, helper)
                        .build(traversal, resolveStartVertex(traversal, resolver, mappingConfig));
            }
            return new VertexSqlBuilder(resolver, where, helper)
                    .build(traversal, mappingConfig);
        }
        return new EdgeSqlBuilder(resolver, where, helper)
                .build(traversal, mappingConfig);
    }

    private com.graphqueryengine.mapping.VertexMapping resolveStartVertex(
            ParsedTraversal traversal, SqlMappingResolver resolver, MappingConfig mappingConfig) {
        String label = traversal.label();
        if (label == null) label = resolver.resolveSingleLabel(mappingConfig.vertices(), "vertex");
        com.graphqueryengine.mapping.VertexMapping vm = mappingConfig.vertices().get(label);
        if (vm == null)
            throw new IllegalArgumentException("No vertex mapping found for label: " + label);
        return vm;
    }

    private QueryPlanBuilder makePlanBuilder(MappingConfig mappingConfig) {
        return new QueryPlanBuilder(new SqlMappingResolver(mappingConfig), dialect);
    }
}
