package com.graphqueryengine.query.translate.sql.render;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.translate.sql.mapping.SqlMappingResolver;
import com.graphqueryengine.query.translate.sql.model.ChooseProjectionSpec;
import com.graphqueryengine.query.translate.sql.model.HopStep;
import com.graphqueryengine.query.translate.sql.model.ParsedTraversal;
import com.graphqueryengine.query.translate.sql.model.ProjectionField;
import com.graphqueryengine.query.translate.sql.model.ProjectionKind;
import com.graphqueryengine.query.translate.sql.parse.GremlinStepParser;
import com.graphqueryengine.query.translate.sql.dialect.SqlDialect;
import com.graphqueryengine.query.translate.sql.constant.GremlinToken;
import com.graphqueryengine.query.translate.sql.constant.SqlFragment;
import com.graphqueryengine.query.translate.sql.constant.SqlKeyword;
import com.graphqueryengine.query.translate.sql.constant.SqlOperator;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Stateless helper providing common SQL fragment-building utilities shared across all
 * SQL builder components.
 */
public class SqlRenderHelper {

    private final SqlDialect dialect;
    private final SqlMappingResolver resolver;
    private final GremlinStepParser parser;

    public SqlRenderHelper(SqlDialect dialect, SqlMappingResolver resolver, GremlinStepParser parser) {
        this.dialect  = dialect;
        this.resolver = resolver;
        this.parser   = parser;
    }

    // ── Dialect access ────────────────────────────────────────────────────────

    /** Returns the SQL dialect configured for this translator instance. */
    public SqlDialect dialect() { return dialect; }

    // ── Limit / Order ─────────────────────────────────────────────────────────

    public static void appendLimit(StringBuilder sql, Integer limit) {
        if (limit != null) sql.append(SqlKeyword.LIMIT).append(limit);
    }

    public void appendOrderBy(StringBuilder sql, String orderByProperty, String orderDirection) {
        if (orderByProperty != null) {
            String direction = orderDirection != null ? orderDirection : SqlKeyword.ASC;
            // Always quote the ORDER BY token so H2 (and other case-sensitive databases)
            // match the double-quoted column alias emitted in the SELECT clause exactly.
            // Raw column names are also safe to double-quote; H2 treats them case-insensitively
            // when unquoted but matches them exactly when quoted.
            sql.append(SqlKeyword.ORDER_BY).append(dialect.quoteIdentifier(orderByProperty))
               .append(SqlFragment.SPACE).append(direction);
        }
    }

    // ── Identifier quoting ────────────────────────────────────────────────────

    public String quoteAlias(String alias) {
        return dialect.quoteIdentifier(alias);
    }

    // ── Numeric / constant literals ───────────────────────────────────────────

    public String sanitizeNumericLiteral(String raw) {
        String value = raw == null ? SqlFragment.EMPTY : raw.trim();
        if (!value.matches("-?\\d+(\\.\\d+)?"))
            throw new IllegalArgumentException("where(...).is(gt/gte(...)) expects a numeric literal");
        return value;
    }

    public String toSqlOperator(String predicate) {
        return switch (predicate) {
            case GremlinToken.PRED_GT  -> SqlOperator.GT;
            case GremlinToken.PRED_GTE -> SqlOperator.GTE;
            case GremlinToken.PRED_LT  -> SqlOperator.LT;
            case GremlinToken.PRED_LTE -> SqlOperator.LTE;
            case GremlinToken.PRED_EQ  -> SqlOperator.EQ;
            case GremlinToken.PRED_NEQ -> SqlOperator.NEQ;
            default -> throw new IllegalArgumentException("Unsupported choose() predicate: " + predicate);
        };
    }

    public String toSqlConstantLiteral(String rawConstant) {
        String trimmed = rawConstant == null ? SqlFragment.EMPTY : rawConstant.trim();
        if (trimmed.isEmpty())
            throw new IllegalArgumentException("choose(...).constant(...) must not be empty");
        if ((trimmed.startsWith(SqlFragment.DOUBLE_QUOTE) && trimmed.endsWith(SqlFragment.DOUBLE_QUOTE))
                || (trimmed.startsWith(SqlFragment.SQL_QUOTE) && trimmed.endsWith(SqlFragment.SQL_QUOTE))) {
            return SqlFragment.SQL_QUOTE + parser.unquote(trimmed).replace(SqlFragment.SQL_QUOTE, SqlFragment.SQL_ESCAPED_QUOTE) + SqlFragment.SQL_QUOTE;
        }
        if (trimmed.matches("-?\\d+(\\.\\d+)?")) return sanitizeNumericLiteral(trimmed);
        if (GremlinToken.BOOL_TRUE.equalsIgnoreCase(trimmed) || GremlinToken.BOOL_FALSE.equalsIgnoreCase(trimmed))
            return trimmed.toUpperCase();
        throw new IllegalArgumentException(
                "choose(...,constant(x),constant(y)) supports quoted string, numeric, or boolean constants only");
    }

    public ChooseProjectionSpec parseChooseProjection(String expression) {
        return parser.parseChooseProjection(expression);
    }

    // ── ValueMap select fragments ─────────────────────────────────────────────

    public String buildAliasValueMapSelectForVertex(VertexMapping vertexMapping, String alias,
                                                     java.util.List<String> keys) {
        StringJoiner selectCols = new StringJoiner(SqlFragment.COMMA_SPACE);
        for (Map.Entry<String, String> entry : vertexMapping.properties().entrySet()) {
            if (keys.isEmpty() || keys.contains(entry.getKey())) {
                selectCols.add(alias + SqlFragment.DOT + entry.getValue() + SqlKeyword.AS + quoteAlias(entry.getKey()));
            }
        }
        return selectCols.toString();
    }

    public String buildAliasValueMapSelectForEdge(EdgeMapping edgeMapping, String alias) {
        StringJoiner selectCols = new StringJoiner(SqlFragment.COMMA_SPACE);
        for (Map.Entry<String, String> entry : edgeMapping.properties().entrySet()) {
            selectCols.add(alias + SqlFragment.DOT + entry.getValue() + SqlKeyword.AS + quoteAlias(entry.getKey()));
        }
        return selectCols.toString();
    }

    // ── Edge endpoint select clause ───────────────────────────────────────────

    public String buildEdgeEndpointSelectClause(ParsedTraversal parsed, VertexMapping endpointMapping) {
        if (!parsed.projections().isEmpty()) {
            StringJoiner projectionSelect = new StringJoiner(SqlFragment.COMMA_SPACE);
            for (ProjectionField projection : parsed.projections()) {
                String alias = quoteAlias(projection.alias());
                if (projection.kind() == ProjectionKind.IDENTITY) {
                    projectionSelect.add(SqlFragment.PREFIX_V + endpointMapping.idColumn() + SqlKeyword.AS + alias);
                    continue;
                }
                if (projection.kind() != ProjectionKind.EDGE_PROPERTY)
                    throw new IllegalArgumentException(
                            "outV()/inV()/bothV() projection supports by('property') and by(identity()) only");
                projectionSelect.add(SqlFragment.PREFIX_V + resolver.mapVertexProperty(endpointMapping, projection.property())
                        + SqlKeyword.AS + alias);
            }
            return projectionSelect.toString();
        }
        if (parsed.valueMapRequested()) return buildAliasValueMapSelectForVertex(endpointMapping, SqlFragment.ALIAS_V, parsed.valueMapKeys());
        if (parsed.valueProperty() != null) {
            String column = resolver.mapVertexProperty(endpointMapping, parsed.valueProperty());
            return SqlFragment.PREFIX_V + column + SqlKeyword.AS + parsed.valueProperty();
        }
        return SqlFragment.PREFIX_V + SqlKeyword.STAR;
    }

    // ── Path select clause ────────────────────────────────────────────────────

    public String buildPathSelectClause(List<String> pathByProps, int hopCount,
                                         VertexMapping startVertexMapping, List<HopStep> hops) {
        StringJoiner pathSelect = new StringJoiner(SqlFragment.COMMA_SPACE);
        List<VertexMapping> hopMappings = new java.util.ArrayList<>();
        hopMappings.add(startVertexMapping);
        VertexMapping prev = startVertexMapping;

        for (HopStep hop : hops) {
            if (hop.labels().isEmpty()) {
                hopMappings.add(prev);
            } else {
                EdgeMapping em = resolver.resolveEdgeMapping(hop.labels().get(0));
                VertexMapping target = resolver.resolveHopTargetVertexMapping(em, GremlinToken.OUT.equals(hop.direction()), prev);
                hopMappings.add(target);
                prev = target;
            }
        }

        int numProps = pathByProps.size();
        for (int i = 0; i <= hopCount; i++) {
            String prop = pathByProps.get(Math.min(i, numProps - 1));
            if (i > 0) {
                HopStep prevHop = hops.get(i - 1);
                if (GremlinToken.OUT_E.equals(prevHop.direction()) || GremlinToken.IN_E.equals(prevHop.direction())) {
                    EdgeMapping em = resolver.resolveEdgeMapping(prevHop.singleLabel());
                    String edgeCol = em.properties().get(prop);
                    pathSelect.add(edgeCol != null
                            ? SqlFragment.E_PREFIX + i + SqlFragment.DOT + edgeCol + SqlKeyword.AS + prop + i
                            : SqlKeyword.NULL_LITERAL + SqlKeyword.AS + prop + i);
                    continue;
                }
            }
            VertexMapping vm = hopMappings.get(Math.min(i, hopMappings.size() - 1));
            String mappedColumn = vm.properties().get(prop);
            if (mappedColumn == null) {
                for (VertexMapping candidate : resolver.mappingConfig().vertices().values()) {
                    if (candidate.properties().containsKey(prop)) {
                        mappedColumn = candidate.properties().get(prop);
                        break;
                    }
                }
            }
            pathSelect.add(mappedColumn != null
                    ? SqlFragment.V_PREFIX + i + SqlFragment.DOT + mappedColumn + SqlKeyword.AS + prop + i
                    : SqlKeyword.NULL_LITERAL + SqlKeyword.AS + prop + i);
        }
        return SqlKeyword.SELECT + pathSelect;
    }

    // ── Aggregate expressions ─────────────────────────────────────────────────

    public String buildHopSumExpression(ParsedTraversal parsed, VertexMapping startVertexMapping,
                                         String finalVertexAlias) {
        if (parsed.valueProperty() == null)
            throw new IllegalArgumentException("sum() requires values('property') before aggregation");
        if (!parsed.selectFields().isEmpty()) {
            String alias = parsed.selectFields().get(0).alias();
            Integer hopIndex = null;
            for (var asAlias : parsed.asAliases()) {
                if (asAlias.label().equals(alias)) { hopIndex = asAlias.hopIndexAfter(); break; }
            }
            if (hopIndex != null && hopIndex > 0 && hopIndex <= parsed.hops().size()) {
                HopStep hop = parsed.hops().get(hopIndex - 1);
                if (GremlinToken.OUT_E.equals(hop.direction()) || GremlinToken.IN_E.equals(hop.direction())
                        || GremlinToken.BOTH_E.equals(hop.direction()) || GremlinToken.OUT.equals(hop.direction())
                        || GremlinToken.IN.equals(hop.direction())) {
                    EdgeMapping em = resolver.resolveEdgeMapping(hop.singleLabel());
                    return SqlKeyword.SUM_FN + SqlFragment.E_PREFIX + hopIndex + SqlFragment.DOT + resolver.mapEdgeProperty(em, parsed.valueProperty()) + SqlKeyword.SUM_ALIAS;
                }
            }
        }
        try {
            return SqlKeyword.SUM_FN + finalVertexAlias + SqlFragment.DOT + resolver.mapVertexProperty(startVertexMapping, parsed.valueProperty()) + SqlKeyword.SUM_ALIAS;
        } catch (IllegalArgumentException ex) {
            if (!parsed.hops().isEmpty()) {
                VertexMapping finalVm = resolver.resolveFinalHopVertexMapping(parsed.hops(), startVertexMapping);
                return SqlKeyword.SUM_FN + finalVertexAlias + SqlFragment.DOT + resolver.mapVertexProperty(finalVm, parsed.valueProperty()) + SqlKeyword.SUM_ALIAS;
            }
            throw ex;
        }
    }

    public String buildHopMeanExpression(ParsedTraversal parsed, VertexMapping startVertexMapping,
                                          String finalVertexAlias) {
        if (parsed.valueProperty() == null)
            throw new IllegalArgumentException("mean() requires values('property') before aggregation");

        // If the last hop is an edge step (outE/inE/bothE), resolve the property from
        // the edge mapping and use the final edge alias (eN), not the vertex alias.
        if (!parsed.hops().isEmpty()) {
            HopStep lastHop = parsed.hops().get(parsed.hops().size() - 1);
            if (GremlinToken.OUT_E.equals(lastHop.direction())
                    || GremlinToken.IN_E.equals(lastHop.direction())
                    || GremlinToken.BOTH_E.equals(lastHop.direction())) {
                EdgeMapping em = resolver.resolveEdgeMapping(lastHop.singleLabel());
                String edgeAlias = SqlFragment.E_PREFIX + parsed.hops().size();
                return SqlKeyword.AVG_FN + edgeAlias + SqlFragment.DOT + resolver.mapEdgeProperty(em, parsed.valueProperty()) + SqlKeyword.MEAN_ALIAS;
            }
        }

        // Vertex-based mean: try start vertex first, then final hop vertex.
        try {
            return SqlKeyword.AVG_FN + finalVertexAlias + SqlFragment.DOT + resolver.mapVertexProperty(startVertexMapping, parsed.valueProperty()) + SqlKeyword.MEAN_ALIAS;
        } catch (IllegalArgumentException ex) {
            if (!parsed.hops().isEmpty()) {
                VertexMapping finalVm = resolver.resolveFinalHopVertexMapping(parsed.hops(), startVertexMapping);
                return SqlKeyword.AVG_FN + finalVertexAlias + SqlFragment.DOT + resolver.mapVertexProperty(finalVm, parsed.valueProperty()) + SqlKeyword.MEAN_ALIAS;
            }
            throw ex;
        }
    }

    // ── Dialect-delegating helpers ────────────────────────────────────────────

    /** Returns a dialect-aware string aggregation expression (e.g., {@code STRING_AGG(expr, ',')}). */
    public String buildNeighborAggExpr(String columnExpr) {
        return dialect.stringAgg(columnExpr, ",");
    }

    // ── Simple-path cycle conditions ──────────────────────────────────────────

    public static StringJoiner buildSimplePathConditions(int hopCount, String idCol) {
        StringJoiner cycleConditions = new StringJoiner(SqlKeyword.AND);
        for (int i = 1; i <= hopCount; i++) {
            String currentAlias = SqlFragment.V_PREFIX + i + SqlFragment.DOT + idCol;
            if (i == 1) {
                cycleConditions.add(currentAlias + SqlKeyword.NEQ + SqlFragment.V0 + SqlFragment.DOT + idCol);
            } else {
                StringJoiner priorAliases = new StringJoiner(SqlFragment.COMMA_SPACE);
                for (int j = 0; j < i; j++) priorAliases.add(SqlFragment.V_PREFIX + j + SqlFragment.DOT + idCol);
                cycleConditions.add(currentAlias + SqlKeyword.NOT_IN + priorAliases + SqlFragment.CLOSE_PAREN);
            }
        }
        return cycleConditions;
    }
}

