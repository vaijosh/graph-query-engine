package com.graphqueryengine.query.translate.sql.where;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.translate.sql.HasFilter;
import com.graphqueryengine.query.translate.sql.constant.GremlinToken;
import com.graphqueryengine.query.translate.sql.constant.SqlFragment;
import com.graphqueryengine.query.translate.sql.constant.SqlKeyword;
import com.graphqueryengine.query.translate.sql.constant.SqlOperator;
import com.graphqueryengine.query.translate.sql.dialect.SqlDialect;
import com.graphqueryengine.query.translate.sql.dialect.StandardSqlDialect;
import com.graphqueryengine.query.translate.sql.mapping.SqlMappingResolver;
import com.graphqueryengine.query.translate.sql.model.AsAlias;
import com.graphqueryengine.query.translate.sql.model.HopStep;
import com.graphqueryengine.query.translate.sql.model.ParsedTraversal;
import com.graphqueryengine.query.translate.sql.model.WhereClause;

import java.util.List;
import java.util.StringJoiner;

/**
 * Builds SQL {@code WHERE} clause fragments from parsed Gremlin filter/where constructs.
 *
 * <p>This class handles:
 * <ul>
 *   <li>Simple {@code has()} / {@code hasId()} / {@code hasNot()} filters on vertex and edge tables</li>
 *   <li>Structured {@code where()} predicates: EDGE_EXISTS, OUT/IN_NEIGHBOR_HAS, AND, OR, NOT</li>
 *   <li>Alias-based predicates: EQ_ALIAS, NEQ_ALIAS</li>
 *   <li>Hop-traversal filter fallback resolution</li>
 * </ul>
 */
public class WhereClauseBuilder {

    private final SqlMappingResolver resolver;
    private final SqlDialect dialect;

    /** Construct with an explicit dialect (used to coerce parameter types). */
    public WhereClauseBuilder(SqlMappingResolver resolver, SqlDialect dialect) {
        this.resolver = resolver;
        this.dialect  = dialect;
    }

    /** Backwards-compatible constructor; defaults to Standard SQL dialect (no coercion). */
    public WhereClauseBuilder(SqlMappingResolver resolver) {
        this(resolver, new StandardSqlDialect());
    }

    // ── Convenience: add a parameter, coercing its type through the dialect ──

    private void addParam(List<Object> params, Object value) {
        params.add(dialect.coerceParamValue(value));
    }

    /**
     * Add a parameter that is known to be bound to a BIGINT/INTEGER id column.
     *
     * <p>When the dialect requires explicit numeric id params (Trino/Iceberg),
     * the value is coerced to {@link Long} regardless of whether it arrived as a String.
     * This handles {@code hasId(1)} and {@code WHERE id = ?} without affecting
     * VARCHAR columns like {@code is_laundering} that also store numeric-looking strings.
     */
    private void addIdParam(List<Object> params, Object value) {
        if (dialect.requiresExplicitNumericIdParams()) {
            params.add(toLong(value));
        } else {
            params.add(value);
        }
    }

    /**
     * Add a parameter that is the result of a COUNT(*) comparison ({@code count().is(n)}).
     * COUNT always returns BIGINT in every SQL database, so this must be Long for Trino.
     */
    private void addCountParam(List<Object> params, Object value) {
        if (dialect.requiresExplicitNumericIdParams()) {
            params.add(toLong(value));
        } else {
            params.add(value);
        }
    }

    /** Convert a value to Long for strict-typed id/count comparisons. */
    private static Long toLong(Object value) {
        if (value instanceof Long l)   return l;
        if (value instanceof Number n) return n.longValue();
        try { return Long.parseLong(String.valueOf(value).trim()); }
        catch (NumberFormatException ignored) { return 0L; }
    }

    // ── Vertex WHERE helpers ──────────────────────────────────────────────────

    public void appendWhereClauseForVertex(StringBuilder sql, List<Object> params,
                                            List<HasFilter> filters, VertexMapping mapping) {
        appendWhereClauseForVertexAlias(sql, params, filters, mapping, null);
    }

    public void appendWhereClauseForVertexAlias(StringBuilder sql, List<Object> params,
                                                  List<HasFilter> filters, VertexMapping mapping,
                                                  String alias) {
        if (filters.isEmpty()) return;
        StringJoiner whereJoiner = new StringJoiner(SqlKeyword.AND);
        for (HasFilter filter : filters) {
            boolean isIdColumn = GremlinToken.PROP_ID.equals(filter.property());
            String column = isIdColumn
                    ? mapping.idColumn()
                    : resolver.mapVertexProperty(mapping, filter.property());
            String qualifiedColumn = alias != null ? alias + SqlFragment.DOT + column : column;
            if (SqlKeyword.IS_NULL_OP.equals(filter.operator())) {
                whereJoiner.add(qualifiedColumn + SqlKeyword.IS_NULL);
            } else {
                whereJoiner.add(qualifiedColumn + SqlFragment.SPACE + filter.operator() + SqlFragment.SPACE + SqlKeyword.PLACEHOLDER);
                // ID columns are always BIGINT → use addIdParam for strict dialects (Trino)
                if (isIdColumn) {
                    addIdParam(params, filter.typedValue());
                } else {
                    addParam(params, filter.typedValue());
                }
            }
        }
        sql.append(SqlKeyword.WHERE).append(whereJoiner);
    }

    // ── Edge WHERE helpers ────────────────────────────────────────────────────

    public void appendWhereClauseForEdge(StringBuilder sql, List<Object> params,
                                          List<HasFilter> filters, EdgeMapping mapping) {
        appendWhereClauseForEdgeAlias(sql, params, filters, mapping, null);
    }

    public void appendWhereClauseForEdgeAlias(StringBuilder sql, List<Object> params,
                                               List<HasFilter> filters, EdgeMapping mapping,
                                               String alias) {
        if (filters.isEmpty()) return;
        StringJoiner whereJoiner = new StringJoiner(SqlKeyword.AND);
        for (HasFilter filter : filters) {
            String column = resolver.mapEdgeFilterProperty(mapping, filter.property());
            String qualifiedColumn = alias == null ? column : alias + SqlFragment.DOT + column;
            if (SqlKeyword.IS_NULL_OP.equals(filter.operator())) {
                whereJoiner.add(qualifiedColumn + SqlKeyword.IS_NULL);
            } else {
                whereJoiner.add(qualifiedColumn + SqlFragment.SPACE + filter.operator() + SqlFragment.SPACE + SqlKeyword.PLACEHOLDER);
                addParam(params, filter.typedValue());
            }
        }
        sql.append(SqlKeyword.WHERE).append(whereJoiner);
    }

    // ── Structured WHERE predicate ────────────────────────────────────────────

    public boolean isStructuredWherePredicate(WhereClause whereClause) {
        return whereClause != null && switch (whereClause.kind()) {
            case EDGE_EXISTS, OUT_NEIGHBOR_HAS, IN_NEIGHBOR_HAS, AND, OR, NOT -> true;
            default -> false;
        };
    }

    public void appendStructuredWherePredicate(StringBuilder sql, List<Object> params,
                                                WhereClause whereClause, VertexMapping vertexMapping,
                                                String vertexAlias, boolean hasExistingWhere) {
        String predicate = buildStructuredWherePredicateSql(whereClause, vertexMapping, vertexAlias, params);
        sql.append(hasExistingWhere ? SqlKeyword.AND : SqlKeyword.WHERE).append(predicate);
    }

    public String buildStructuredWherePredicateSql(WhereClause whereClause, VertexMapping vertexMapping,
                                                    String vertexAlias, List<Object> params) {
        return switch (whereClause.kind()) {
            case EDGE_EXISTS ->
                    buildEdgeExistsPredicateSql(whereClause, vertexMapping, vertexAlias, params);
            case OUT_NEIGHBOR_HAS, IN_NEIGHBOR_HAS ->
                    buildNeighborHasPredicateSql(whereClause, vertexMapping, vertexAlias, params);
            case AND ->
                    joinStructuredPredicates(whereClause.clauses(), SqlKeyword.AND, vertexMapping, vertexAlias, params);
            case OR ->
                    joinStructuredPredicates(whereClause.clauses(), SqlKeyword.OR, vertexMapping, vertexAlias, params);
            case NOT -> {
                if (whereClause.clauses().size() != 1)
                    throw new IllegalArgumentException("where(not(...)) requires exactly one inner predicate");
                yield SqlKeyword.NOT + SqlFragment.OPEN_PAREN + buildStructuredWherePredicateSql(
                        whereClause.clauses().get(0), vertexMapping, vertexAlias, params) + SqlFragment.CLOSE_PAREN;
            }
            default -> throw new IllegalArgumentException(
                    "Unsupported structured where() predicate: " + whereClause.kind());
        };
    }

    private String joinStructuredPredicates(List<WhereClause> clauses, String delimiter,
                                             VertexMapping vertexMapping, String vertexAlias,
                                             List<Object> params) {
        if (clauses == null || clauses.isEmpty())
            throw new IllegalArgumentException("Compound where() predicate requires inner predicates");
        StringJoiner joiner = new StringJoiner(delimiter);
        for (WhereClause clause : clauses) {
            joiner.add(SqlFragment.OPEN_PAREN + buildStructuredWherePredicateSql(clause, vertexMapping, vertexAlias, params) + SqlFragment.CLOSE_PAREN);
        }
        return SqlFragment.OPEN_PAREN + joiner + SqlFragment.CLOSE_PAREN;
    }

    private String buildEdgeExistsPredicateSql(WhereClause whereClause, VertexMapping vertexMapping,
                                                String vertexAlias, List<Object> params) {
        EdgeMapping edgeMapping = resolver.resolveEdgeMapping(whereClause.right());
        String edgeAlias = SqlFragment.ALIAS_WE_CORR;
        String idRef = vertexAlias == null
                ? vertexMapping.idColumn()
                : vertexAlias + SqlFragment.DOT + vertexMapping.idColumn();
        String correlation;
        if (GremlinToken.OUT_E.equals(whereClause.left())) {
            correlation = edgeAlias + SqlFragment.DOT + edgeMapping.outColumn() + SqlFragment.SPACE_EQ_SPACE + idRef;
        } else if (GremlinToken.IN_E.equals(whereClause.left())) {
            correlation = edgeAlias + SqlFragment.DOT + edgeMapping.inColumn() + SqlFragment.SPACE_EQ_SPACE + idRef;
        } else {
            correlation = SqlFragment.OPEN_PAREN + edgeAlias + SqlFragment.DOT + edgeMapping.outColumn() + SqlFragment.SPACE_EQ_SPACE + idRef
                    + SqlKeyword.OR + edgeAlias + SqlFragment.DOT + edgeMapping.inColumn() + SqlFragment.SPACE_EQ_SPACE + idRef + SqlFragment.CLOSE_PAREN;
        }

        // Count comparison: outE('LABEL').count().is(n)
        HasFilter countFilter = whereClause.filters().stream()
                .filter(f -> GremlinToken.PROP_COUNT.equals(f.property())).findFirst().orElse(null);
        if (countFilter != null) {
            StringBuilder countSql = new StringBuilder(SqlFragment.OPEN_PAREN).append(SqlKeyword.SELECT_COUNT)
                    .append(edgeMapping.table()).append(SqlFragment.SPACE).append(edgeAlias)
                    .append(SqlKeyword.WHERE).append(correlation);
            for (HasFilter filter : whereClause.filters()) {
                if (!GremlinToken.PROP_COUNT.equals(filter.property())) {
                    String column = resolver.mapEdgeFilterProperty(edgeMapping, filter.property());
                    if (SqlKeyword.IS_NULL_OP.equals(filter.operator())) {
                        countSql.append(SqlKeyword.AND).append(edgeAlias).append(SqlFragment.DOT).append(column).append(SqlKeyword.IS_NULL);
                    } else {
                        countSql.append(SqlKeyword.AND).append(edgeAlias).append(SqlFragment.DOT).append(column)
                                .append(SqlFragment.SPACE).append(filter.operator()).append(SqlFragment.SPACE).append(SqlKeyword.PLACEHOLDER);
                        addParam(params, filter.typedValue());
                    }
                }
            }
            countSql.append(SqlFragment.CLOSE_PAREN).append(SqlFragment.SPACE).append(toSqlOperator(countFilter.operator())).append(SqlFragment.SPACE).append(SqlKeyword.PLACEHOLDER);
            // COUNT(*) always returns BIGINT — use addCountParam for strict dialects (Trino)
            addCountParam(params, countFilter.typedValue());
            return countSql.toString();
        }

        // Standard EXISTS clause
        StringBuilder existsSql = new StringBuilder(SqlKeyword.EXISTS + SqlFragment.OPEN_PAREN)
                .append(SqlKeyword.SELECT_1)
                .append(edgeMapping.table()).append(SqlFragment.SPACE).append(edgeAlias)
                .append(SqlKeyword.WHERE).append(correlation);
        for (HasFilter filter : whereClause.filters()) {
            String column = resolver.mapEdgeFilterProperty(edgeMapping, filter.property());
            if (SqlKeyword.IS_NULL_OP.equals(filter.operator())) {
                existsSql.append(SqlKeyword.AND).append(edgeAlias).append(SqlFragment.DOT).append(column).append(SqlKeyword.IS_NULL);
            } else {
                existsSql.append(SqlKeyword.AND).append(edgeAlias).append(SqlFragment.DOT).append(column)
                        .append(SqlFragment.SPACE).append(filter.operator()).append(SqlFragment.SPACE).append(SqlKeyword.PLACEHOLDER);
                addParam(params, filter.typedValue());
            }
        }
        return existsSql.append(SqlFragment.CLOSE_PAREN).toString();
    }

    private String buildNeighborHasPredicateSql(WhereClause whereClause, VertexMapping vertexMapping,
                                                  String vertexAlias, List<Object> params) {
        EdgeMapping edgeMapping = resolver.resolveEdgeMapping(whereClause.right());
        boolean isOut = GremlinToken.OUT.equals(whereClause.left());
        String anchorCol = isOut ? edgeMapping.outColumn() : edgeMapping.inColumn();
        String targetCol = isOut ? edgeMapping.inColumn() : edgeMapping.outColumn();
        VertexMapping neighborVertexMapping = resolver.resolveTargetVertexMapping(edgeMapping, isOut);

        boolean missingFilterProps = false;
        if (neighborVertexMapping != null) {
            for (HasFilter filter : whereClause.filters()) {
                if (!neighborVertexMapping.properties().containsKey(filter.property())) {
                    missingFilterProps = true;
                    break;
                }
            }
        }
        if (neighborVertexMapping == null || missingFilterProps) {
            List<String> filterProps = whereClause.filters().stream().map(HasFilter::property).toList();
            VertexMapping propMatched = resolver.resolveVertexMappingByProperties(filterProps);
            if (propMatched != null) neighborVertexMapping = propMatched;
        }
        if (neighborVertexMapping == null) neighborVertexMapping = vertexMapping;

        String idRef = vertexAlias == null
                ? vertexMapping.idColumn()
                : vertexAlias + SqlFragment.DOT + vertexMapping.idColumn();
        StringBuilder existsSql = new StringBuilder(SqlKeyword.EXISTS + SqlFragment.OPEN_PAREN)
                .append(SqlKeyword.SELECT_1)
                .append(edgeMapping.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_WE)
                .append(SqlKeyword.JOIN).append(neighborVertexMapping.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_WV)
                .append(SqlKeyword.ON).append(SqlFragment.ALIAS_WV).append(SqlFragment.DOT).append(neighborVertexMapping.idColumn()).append(SqlFragment.SPACE_EQ_SPACE).append(SqlFragment.ALIAS_WE).append(SqlFragment.DOT).append(targetCol)
                .append(SqlKeyword.WHERE).append(SqlFragment.ALIAS_WE).append(SqlFragment.DOT).append(anchorCol).append(SqlFragment.SPACE_EQ_SPACE).append(idRef);

        for (HasFilter filter : whereClause.filters()) {
            String column = resolver.mapVertexProperty(neighborVertexMapping, filter.property());
            if (SqlKeyword.IS_NULL_OP.equals(filter.operator())) {
                existsSql.append(SqlKeyword.AND).append(SqlFragment.ALIAS_WV).append(SqlFragment.DOT).append(column).append(SqlKeyword.IS_NULL);
            } else {
                existsSql.append(SqlKeyword.AND).append(SqlFragment.ALIAS_WV).append(SqlFragment.DOT).append(column)
                        .append(SqlFragment.SPACE).append(filter.operator()).append(SqlFragment.SPACE).append(SqlKeyword.PLACEHOLDER);
                addParam(params, filter.typedValue());
            }
        }
        existsSql.append(SqlFragment.CLOSE_PAREN);
        return existsSql.toString();
    }

    // ── Alias-based WHERE predicates ──────────────────────────────────────────

    public void appendEqAliasPredicate(StringBuilder sql, WhereClause whereClause,
                                        List<AsAlias> asAliases, String currentAlias,
                                        String idCol, boolean hasExistingWhere) {
        String targetAlias = whereClause.left();
        Integer hopIndex = null;
        for (AsAlias aa : asAliases) {
            if (aa.label().equals(targetAlias)) { hopIndex = aa.hopIndexAfter(); break; }
        }
        if (hopIndex == null)
            throw new IllegalArgumentException(
                    "where(eq('alias')): alias '" + targetAlias + "' not defined via .as() in the traversal");
        String targetVertexAlias = SqlFragment.V_PREFIX + hopIndex;
        sql.append(hasExistingWhere ? SqlKeyword.AND : SqlKeyword.WHERE)
                .append(currentAlias).append(SqlFragment.DOT).append(idCol)
                .append(SqlFragment.SPACE_EQ_SPACE).append(targetVertexAlias).append(SqlFragment.DOT).append(idCol);
    }

    public void appendNeqAliasPredicate(StringBuilder sql, WhereClause whereClause,
                                         List<AsAlias> asAliases, String whereByProperty,
                                         List<HopStep> hops, VertexMapping startVertexMapping,
                                         String idCol, boolean hasExistingWhere) {
        int leftHop  = resolver.resolveAliasHopIndex(whereClause.left(), asAliases, "left");
        int rightHop = resolver.resolveAliasHopIndex(whereClause.right(), asAliases, "right");
        String leftAlias  = SqlFragment.V_PREFIX + leftHop;
        String rightAlias = SqlFragment.V_PREFIX + rightHop;
        String predicate;
        if (whereByProperty == null || whereByProperty.isBlank()) {
            predicate = leftAlias + SqlFragment.DOT + idCol + SqlKeyword.NEQ + rightAlias + SqlFragment.DOT + idCol;
        } else {
            VertexMapping leftMapping  = resolver.resolveVertexMappingAtHop(hops, startVertexMapping, leftHop);
            VertexMapping rightMapping = resolver.resolveVertexMappingAtHop(hops, startVertexMapping, rightHop);
            String leftCol  = resolver.mapVertexProperty(leftMapping, whereByProperty);
            String rightCol = resolver.mapVertexProperty(rightMapping, whereByProperty);
            predicate = leftAlias + SqlFragment.DOT + leftCol + SqlKeyword.NEQ + rightAlias + SqlFragment.DOT + rightCol;
        }
        sql.append(hasExistingWhere ? SqlKeyword.AND : SqlKeyword.WHERE).append(predicate);
    }

    // ── Hop filter fallback ───────────────────────────────────────────────────

    /**
     * Appends {@code has()} filters for a hop traversal, trying the start vertex first,
     * then the final vertex, then any terminal edge mapping.
     *
     * @return {@code true} if a WHERE clause was appended
     */
    public boolean appendHopFiltersWithTargetFallback(StringBuilder sql, List<Object> params,
                                                       List<HasFilter> filters,
                                                       VertexMapping startVertexMapping,
                                                       String finalVertexAlias,
                                                       VertexMapping finalVertexMapping,
                                                       ParsedTraversal parsed) {
        boolean where = false;
        HopStep lastHop = parsed.hops().isEmpty() ? null : parsed.hops().get(parsed.hops().size() - 1);
        EdgeMapping terminalEdgeMapping = null;
        String terminalEdgeAlias = null;

        // Determine which hop carries the edge (handles outE().has().inV() where lastHop is IN_V)
        HopStep edgeCarryingHop = lastHop;
        int edgeCarryingHopIndex = parsed.hops().size();
        if (lastHop != null && (GremlinToken.IN_V.equals(lastHop.direction())
                || GremlinToken.OUT_V.equals(lastHop.direction()))
                && parsed.hops().size() >= 2) {
            edgeCarryingHop = parsed.hops().get(parsed.hops().size() - 2);
            edgeCarryingHopIndex = parsed.hops().size() - 1;
        }
        if (edgeCarryingHop != null && (GremlinToken.OUT_E.equals(edgeCarryingHop.direction())
                || GremlinToken.IN_E.equals(edgeCarryingHop.direction())
                || GremlinToken.BOTH_E.equals(edgeCarryingHop.direction()))) {
            terminalEdgeMapping = resolver.resolveEdgeMapping(edgeCarryingHop.singleLabel());
            terminalEdgeAlias = SqlFragment.E_PREFIX + edgeCarryingHopIndex;
        }
        // Also try edge mapping for the last normalized hop (handles outE().has().inV() normalized to out())
        if (terminalEdgeMapping == null && lastHop != null && !lastHop.labels().isEmpty()) {
            try {
                terminalEdgeMapping = resolver.resolveEdgeMapping(lastHop.singleLabel());
                terminalEdgeAlias = SqlFragment.E_PREFIX + parsed.hops().size();
            } catch (IllegalArgumentException ignored) {
                terminalEdgeMapping = null;
                terminalEdgeAlias = null;
            }
        }

        for (HasFilter filter : filters) {
            String alias = SqlFragment.V0;
            String mappedColumn;
            if (GremlinToken.PROP_ID.equals(filter.property())) {
                mappedColumn = startVertexMapping.idColumn();
            } else {
                try {
                    mappedColumn = resolver.mapVertexProperty(startVertexMapping, filter.property());
                } catch (IllegalArgumentException ex1) {
                    try {
                        mappedColumn = resolver.mapVertexProperty(finalVertexMapping, filter.property());
                        alias = finalVertexAlias;
                    } catch (IllegalArgumentException ex2) {
                        if (terminalEdgeMapping == null) throw ex2;
                        mappedColumn = resolver.mapEdgeProperty(terminalEdgeMapping, filter.property());
                        alias = terminalEdgeAlias;
                    }
                }
            }
            sql.append(where ? SqlKeyword.AND : SqlKeyword.WHERE).append(alias).append(SqlFragment.DOT).append(mappedColumn);
            if (SqlKeyword.IS_NULL_OP.equals(filter.operator())) {
                sql.append(SqlKeyword.IS_NULL);
            } else {
                sql.append(SqlFragment.SPACE).append(filter.operator()).append(SqlFragment.SPACE).append(SqlKeyword.PLACEHOLDER);
                addParam(params, filter.typedValue());
            }
            where = true;
        }
        return where;
    }

    // ── Utility ───────────────────────────────────────────────────────────────

    private String toSqlOperator(String predicate) {
        return switch (predicate) {
            case GremlinToken.PRED_GT  -> SqlOperator.GT;
            case GremlinToken.PRED_GTE -> SqlOperator.GTE;
            case GremlinToken.PRED_LT  -> SqlOperator.LT;
            case GremlinToken.PRED_LTE -> SqlOperator.LTE;
            case GremlinToken.PRED_EQ  -> SqlOperator.EQ;
            case GremlinToken.PRED_NEQ -> SqlOperator.NEQ;
            default -> throw new IllegalArgumentException("Unsupported predicate operator: " + predicate);
        };
    }
}

