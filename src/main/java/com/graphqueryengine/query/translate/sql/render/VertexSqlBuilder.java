package com.graphqueryengine.query.translate.sql.render;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.translate.sql.constant.GremlinToken;
import com.graphqueryengine.query.translate.sql.constant.SqlFragment;
import com.graphqueryengine.query.translate.sql.constant.SqlKeyword;
import com.graphqueryengine.query.translate.sql.constant.SqlOperator;
import com.graphqueryengine.query.translate.sql.mapping.SqlMappingResolver;
import com.graphqueryengine.query.translate.sql.model.AsAlias;
import com.graphqueryengine.query.translate.sql.model.ChooseProjectionSpec;
import com.graphqueryengine.query.translate.sql.model.GroupCountKeySpec;
import com.graphqueryengine.query.translate.sql.model.HopStep;
import com.graphqueryengine.query.translate.sql.model.ParsedTraversal;
import com.graphqueryengine.query.translate.sql.model.ProjectionField;
import com.graphqueryengine.query.translate.sql.model.ProjectionKind;
import com.graphqueryengine.query.translate.sql.where.WhereClauseBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

/**
 * Builds SQL for {@code g.V(...)} (vertex-rooted) traversals that do not involve hops.
 * Hop-based vertex traversals are delegated to {@link HopSqlBuilder}.
 */
public class VertexSqlBuilder {

    private final SqlMappingResolver resolver;
    private final WhereClauseBuilder whereBuilder;
    private final SqlRenderHelper helper;

    public VertexSqlBuilder(SqlMappingResolver resolver, WhereClauseBuilder whereBuilder,
                             SqlRenderHelper helper) {
        this.resolver     = resolver;
        this.whereBuilder = whereBuilder;
        this.helper       = helper;
    }

    // ── Entry point ───────────────────────────────────────────────────────────

    public TranslationResult build(ParsedTraversal parsed, MappingConfig mappingConfig) {
        String label = parsed.label();
        if (label == null) label = resolver.resolveSingleLabel(mappingConfig.vertices(), GremlinToken.TYPE_VERTEX);

        VertexMapping vertexMapping = mappingConfig.vertices().get(label);
        if (vertexMapping == null)
            throw new IllegalArgumentException("No vertex mapping found for label: " + label);

        Integer effectiveLimit = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();

        if (parsed.groupCountProperty() != null)   return buildGroupCountSql(parsed, vertexMapping);
        if (parsed.groupCountKeySpec() != null)    return buildAliasKeyedGroupCountSql(parsed, vertexMapping);
        if (!parsed.projections().isEmpty())        return buildProjectionSql(parsed, mappingConfig, vertexMapping);

        if (parsed.valueMapRequested()) {
            StringJoiner selectCols = new StringJoiner(SqlFragment.COMMA_SPACE);
            List<String> keys = parsed.valueMapKeys();
            for (Map.Entry<String, String> entry : vertexMapping.properties().entrySet()) {
                if (keys.isEmpty() || keys.contains(entry.getKey())) {
                    selectCols.add(entry.getValue() + SqlKeyword.AS + helper.quoteAlias(entry.getKey()));
                }
            }
            List<Object> params = new ArrayList<>();
            StringBuilder sql = new StringBuilder(SqlKeyword.SELECT)
                    .append(parsed.dedupRequested() ? SqlKeyword.DISTINCT : "")
                    .append(selectCols).append(SqlKeyword.FROM).append(vertexMapping.table());
            whereBuilder.appendWhereClauseForVertex(sql, params, parsed.filters(), vertexMapping);
            if (parsed.whereClause() != null && whereBuilder.isStructuredWherePredicate(parsed.whereClause())) {
                whereBuilder.appendStructuredWherePredicate(
                        sql, params, parsed.whereClause(), vertexMapping, null, !parsed.filters().isEmpty());
            }
            helper.appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
            SqlRenderHelper.appendLimit(sql, effectiveLimit);
            return new TranslationResult(sql.toString(), params);
        }

        if (parsed.countRequested() && parsed.sumRequested())
            throw new IllegalArgumentException("count() cannot be combined with sum()");

        return buildSimpleSelectSql(parsed, vertexMapping, effectiveLimit);
    }

    // ── Simple SELECT ─────────────────────────────────────────────────────────

    private TranslationResult buildSimpleSelectSql(ParsedTraversal parsed, VertexMapping vertexMapping,
                                                    Integer effectiveLimit) {
        // Always alias the table as "v" so correlated sub-queries in WHERE EXISTS can
        // unambiguously reference "v.<idColumn>" rather than an unqualified name that
        // may resolve to an inner-table column of the same name (e.g. "id" in an edge table).
        boolean needsAlias = parsed.whereClause() != null
                && whereBuilder.isStructuredWherePredicate(parsed.whereClause());
        String tableRef = needsAlias
                ? vertexMapping.table() + SqlFragment.SPACE + SqlFragment.ALIAS_V
                : vertexMapping.table();
        String vertexAlias = needsAlias ? SqlFragment.ALIAS_V : null;

        String selectClause = resolveSimpleSelectClause(parsed, vertexMapping);
        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder(SqlKeyword.SELECT).append(selectClause)
                .append(SqlKeyword.FROM).append(tableRef);
        whereBuilder.appendWhereClauseForVertexAlias(sql, params, parsed.filters(), vertexMapping, vertexAlias);
        if (parsed.whereClause() != null) {
            if (!whereBuilder.isStructuredWherePredicate(parsed.whereClause()))
                throw new IllegalArgumentException(
                        "Unsupported where() predicate for g.V(): " + parsed.whereClause().kind());
            whereBuilder.appendStructuredWherePredicate(
                    sql, params, parsed.whereClause(), vertexMapping, vertexAlias, !parsed.filters().isEmpty());
        }
        helper.appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
        SqlRenderHelper.appendLimit(sql, effectiveLimit);
        return new TranslationResult(sql.toString(), params);
    }

    private String resolveSimpleSelectClause(ParsedTraversal parsed, VertexMapping vertexMapping) {
        if (parsed.countRequested()) {
            return parsed.dedupRequested()
                    ? SqlKeyword.COUNT_DISTINCT + vertexMapping.idColumn() + SqlFragment.CLOSE_PAREN + SqlKeyword.AS + SqlFragment.ALIAS_COUNT
                    : SqlKeyword.COUNT_STAR;
        }
        if (parsed.sumRequested()) {
            if (parsed.valueProperty() == null)
                throw new IllegalArgumentException("sum() requires values('property') before aggregation");
            return SqlKeyword.SUM_FN + resolver.mapVertexProperty(vertexMapping, parsed.valueProperty()) + SqlKeyword.SUM_ALIAS;
        }
        if (parsed.meanRequested()) {
            if (parsed.valueProperty() == null)
                throw new IllegalArgumentException("mean() requires values('property') before aggregation");
            return SqlKeyword.AVG_FN + resolver.mapVertexProperty(vertexMapping, parsed.valueProperty()) + SqlKeyword.MEAN_ALIAS;
        }
        if (parsed.valueProperty() != null) {
            String mappedColumn = resolver.mapVertexProperty(vertexMapping, parsed.valueProperty());
            return (parsed.dedupRequested() ? SqlKeyword.DISTINCT : "") + mappedColumn + SqlKeyword.AS + parsed.valueProperty();
        }
        return parsed.dedupRequested() ? SqlKeyword.DISTINCT + SqlKeyword.STAR : SqlKeyword.STAR;
    }

    // ── groupCount ────────────────────────────────────────────────────────────

    public TranslationResult buildGroupCountSql(ParsedTraversal parsed, VertexMapping vertexMapping) {
        String groupProperty = parsed.groupCountProperty();
        String mappedColumn = resolver.mapVertexProperty(vertexMapping, groupProperty);
        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder(SqlKeyword.SELECT)
                .append(mappedColumn).append(SqlKeyword.AS).append(helper.quoteAlias(groupProperty))
                .append(SqlFragment.COMMA_SPACE).append(SqlKeyword.COUNT_STAR).append(SqlKeyword.FROM).append(vertexMapping.table());
        whereBuilder.appendWhereClauseForVertex(sql, params, parsed.filters(), vertexMapping);
        return getTranslationResult(parsed, mappedColumn, params, sql, helper);
    }

    static TranslationResult getTranslationResult(ParsedTraversal parsed, String mappedColumn, List<Object> params, StringBuilder sql, SqlRenderHelper helper) {
        sql.append(SqlKeyword.GROUP_BY).append(mappedColumn);
        if (parsed.orderByCountDesc()) {
            sql.append(SqlKeyword.ORDER_BY).append(SqlKeyword.COUNT_ALIAS).append(parsed.orderDirection() != null ? parsed.orderDirection() : SqlKeyword.DESC);
        } else {
            helper.appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
        }
        SqlRenderHelper.appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());
        return new TranslationResult(sql.toString(), params);
    }

    // ── alias-keyed groupCount ────────────────────────────────────────────────

    public TranslationResult buildAliasKeyedGroupCountSql(ParsedTraversal parsed,
                                                          VertexMapping startVertexMapping) {
        GroupCountKeySpec keySpec = parsed.groupCountKeySpec();
        if (keySpec == null)
            throw new IllegalArgumentException("groupCountKeySpec is required for alias-keyed groupCount");

        int aliasHopIndex = -1;
        for (AsAlias aa : parsed.asAliases()) {
            if (aa.label().equals(keySpec.alias())) { aliasHopIndex = aa.hopIndexAfter(); break; }
        }
        if (aliasHopIndex < 0)
            throw new IllegalArgumentException(
                    "Alias '" + keySpec.alias() + "' not found in traversal; use .as('alias') before groupCount()");

        VertexMapping keyVertexMapping = startVertexMapping;
        if (aliasHopIndex != 0) {
            for (int i = 0; i < Math.min(aliasHopIndex, parsed.hops().size()); i++) {
                HopStep hop = parsed.hops().get(i);
                if (!hop.labels().isEmpty()) {
                    EdgeMapping em = resolver.resolveEdgeMapping(hop.labels().get(0));
                    boolean outDirection = GremlinToken.OUT.equals(hop.direction()) || GremlinToken.OUT_E.equals(hop.direction());
                    keyVertexMapping = resolver.resolveHopTargetVertexMapping(em, outDirection, keyVertexMapping);
                }
            }
        }

        String groupPropCol = resolver.mapVertexProperty(keyVertexMapping, keySpec.property());
        String vertexAlias = SqlFragment.V_PREFIX + aliasHopIndex;
        String idCol = startVertexMapping.idColumn();
        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder(SqlKeyword.SELECT)
                .append(vertexAlias).append(SqlFragment.DOT).append(groupPropCol)
                .append(SqlKeyword.AS).append(helper.quoteAlias(keySpec.property()))
                .append(SqlFragment.COMMA_SPACE).append(SqlKeyword.COUNT_STAR)
                .append(SqlKeyword.FROM).append(startVertexMapping.table()).append(SqlFragment.SPACE).append(SqlFragment.V0);

        for (int i = 0; i < parsed.hops().size(); i++) {
            HopStep hop = parsed.hops().get(i);
            EdgeMapping edgeMapping = resolver.resolveEdgeMapping(hop.singleLabel());
            String edgeAlias  = SqlFragment.E_PREFIX + (i + 1);
            String fromAlias  = SqlFragment.V_PREFIX + i;
            String toAlias    = SqlFragment.V_PREFIX + (i + 1);
            String fromId     = fromAlias + SqlFragment.DOT + idCol;
            String toId       = toAlias + SqlFragment.DOT + idCol;
            boolean outDirection = GremlinToken.OUT.equals(hop.direction()) || GremlinToken.OUT_E.equals(hop.direction());
            sql.append(SqlKeyword.JOIN).append(edgeMapping.table()).append(SqlFragment.SPACE).append(edgeAlias);
            if (outDirection)
                sql.append(SqlKeyword.ON).append(edgeAlias).append(SqlFragment.DOT).append(edgeMapping.outColumn()).append(SqlFragment.SPACE_EQ_SPACE).append(fromId);
            else
                sql.append(SqlKeyword.ON).append(edgeAlias).append(SqlFragment.DOT).append(edgeMapping.inColumn()).append(SqlFragment.SPACE_EQ_SPACE).append(fromId);
            VertexMapping toVm = resolver.resolveHopTargetVertexMapping(edgeMapping, outDirection, startVertexMapping);
            sql.append(SqlKeyword.JOIN).append(toVm.table()).append(SqlFragment.SPACE).append(toAlias);
            if (outDirection)
                sql.append(SqlKeyword.ON).append(toId).append(SqlFragment.SPACE_EQ_SPACE).append(edgeAlias).append(SqlFragment.DOT).append(edgeMapping.inColumn());
            else
                sql.append(SqlKeyword.ON).append(toId).append(SqlFragment.SPACE_EQ_SPACE).append(edgeAlias).append(SqlFragment.DOT).append(edgeMapping.outColumn());
        }

        whereBuilder.appendWhereClauseForVertexAlias(sql, params, parsed.filters(), startVertexMapping, SqlFragment.V0);
        sql.append(SqlKeyword.GROUP_BY).append(vertexAlias).append(SqlFragment.DOT).append(groupPropCol);
        if (parsed.orderByCountDesc()) {
            sql.append(SqlKeyword.ORDER_BY).append(SqlKeyword.COUNT_ALIAS).append(parsed.orderDirection() != null ? parsed.orderDirection() : SqlKeyword.DESC);
        } else {
            helper.appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
        }
        SqlRenderHelper.appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());
        return new TranslationResult(sql.toString(), params);
    }

    // ── project(...) ──────────────────────────────────────────────────────────

    /**
     * Descriptor for a single pre-aggregated degree CTE.
     *
     * <p>For {@code outE('TRANSFER').count()} the key is {@code "out_id:aml_transfers:"}
     * (no filter suffix). For {@code outE('TRANSFER').has('isLaundering','1').count()} it
     * becomes {@code "out_id:aml_transfers:is_laundering='1'"}.  Two projections that share
     * the same key share one CTE.
     *
     * @param cteName    SQL alias for this CTE, e.g. {@code _deg0}
     * @param edgeTable  edge table name
     * @param joinCol    the join column (outColumn or inColumn)
     * @param filterSql  optional extra {@code AND ...} fragment (empty string = no filter)
     */
    private record DegCte(String cteName, String edgeTable, String joinCol, String filterSql) {}

    public TranslationResult buildProjectionSql(ParsedTraversal parsed, MappingConfig mappingConfig,
                                                 VertexMapping vertexMapping) {
        if (parsed.countRequested())
            throw new IllegalArgumentException("count() cannot be combined with project(...)");
        if (parsed.valueProperty() != null)
            throw new IllegalArgumentException("values() cannot be combined with project(...)");

        List<Object> params = new ArrayList<>();
        StringJoiner selectJoiner = new StringJoiner(SqlFragment.COMMA_SPACE);
        int neighborJoinIdx = 0;

        // ── Collect EDGE_DEGREE CTEs (deduplicated by logical key) ────────────
        // key = "joinCol:edgeTable:filterSql"  →  DegCte
        LinkedHashMap<String, DegCte> degCtesOrdered = new LinkedHashMap<>();
        // Maps each EDGE_DEGREE projection index → its CTE name (for SELECT column reference)
        Map<Integer, String> projIdxToCte = new LinkedHashMap<>();

        int projIdx = 0;
        for (ProjectionField projection : parsed.projections()) {
            if (projection.kind() == ProjectionKind.EDGE_DEGREE) {
                String[] parts = projection.property().split(SqlFragment.COLON, -1);
                if (parts.length < 2)
                    throw new IllegalArgumentException("Invalid EDGE_DEGREE property: " + projection.property());
                String direction = parts[0];
                String edgeLabel = parts[1];
                EdgeMapping em   = Optional.ofNullable(mappingConfig.edges().get(edgeLabel))
                        .orElseThrow(() -> new IllegalArgumentException("No edge mapping found for label: " + edgeLabel));
                String joinColumn = GremlinToken.OUT.equals(direction) ? em.outColumn() : em.inColumn();

                // Build the optional filter fragment (SQL literal, no bind params)
                StringBuilder filterBuf = new StringBuilder();
                for (int i = 2; i < parts.length; i++) {
                    int eq = parts[i].indexOf(SqlFragment.EQUALS_SIGN);
                    if (eq >= 1) {
                        String filterProp = parts[i].substring(0, eq);
                        String filterVal  = parts[i].substring(eq + 1);
                        filterBuf.append(SqlKeyword.AND)
                                .append(resolver.mapEdgeProperty(em, filterProp))
                                .append(SqlFragment.SPACE_EQ_SPACE)
                                .append(SqlFragment.SQL_QUOTE)
                                .append(filterVal.replace(SqlFragment.SQL_QUOTE,
                                        SqlFragment.SQL_QUOTE + SqlFragment.SQL_QUOTE))
                                .append(SqlFragment.SQL_QUOTE);
                    }
                }
                String filterSql = filterBuf.toString();
                String cteKey    = joinColumn + SqlFragment.COLON + em.table() + SqlFragment.COLON + filterSql;

                DegCte cte = degCtesOrdered.computeIfAbsent(cteKey, k -> {
                    String name = "_deg" + degCtesOrdered.size();
                    return new DegCte(name, em.table(), joinColumn, filterSql);
                });
                projIdxToCte.put(projIdx, cte.cteName());
            }
            projIdx++;
        }

        // ── Build SELECT list ─────────────────────────────────────────────────
        projIdx = 0;
        for (ProjectionField projection : parsed.projections()) {
            String alias = helper.quoteAlias(projection.alias());
            switch (projection.kind()) {
                case IDENTITY ->
                    selectJoiner.add(SqlFragment.PREFIX_V + vertexMapping.idColumn() + SqlKeyword.AS + alias);
                case EDGE_PROPERTY -> {
                    String column = resolver.mapVertexProperty(vertexMapping, projection.property());
                    selectJoiner.add(SqlFragment.PREFIX_V + column + SqlKeyword.AS + alias);
                }
                case CHOOSE_VALUES_IS_CONSTANT -> {
                    ChooseProjectionSpec chooseSpec = helper.parseChooseProjection(projection.property());
                    if (chooseSpec == null)
                        throw new IllegalArgumentException("Unsupported by() projection expression: " + projection.property());
                    String column    = resolver.mapVertexProperty(vertexMapping, chooseSpec.property());
                    String operator  = helper.toSqlOperator(chooseSpec.predicate());
                    String threshold = helper.sanitizeNumericLiteral(chooseSpec.predicateValue());
                    String whenTrue  = helper.toSqlConstantLiteral(chooseSpec.trueConstant());
                    String whenFalse = helper.toSqlConstantLiteral(chooseSpec.falseConstant());
                    selectJoiner.add(SqlKeyword.CASE_WHEN + SqlFragment.PREFIX_V + column + SqlFragment.SPACE
                            + operator + SqlFragment.SPACE + threshold
                            + SqlKeyword.THEN + whenTrue + SqlKeyword.ELSE + whenFalse
                            + SqlKeyword.END + SqlKeyword.AS + alias);
                }
                case EDGE_DEGREE -> {
                    // Reference the pre-aggregated CTE column instead of a correlated subquery
                    String cteName = projIdxToCte.get(projIdx);
                    selectJoiner.add("COALESCE(" + cteName + "._cnt, 0)" + SqlKeyword.AS + alias);
                }
                case OUT_VERTEX_COUNT, IN_VERTEX_COUNT -> {
                    String[] parts = projection.property().split(SqlFragment.COLON, -1);
                    if (parts.length < 2)
                        throw new IllegalArgumentException("Invalid VERTEX_COUNT property: " + projection.property());
                    String direction  = parts[0];
                    String edgeLabel  = parts[1];
                    EdgeMapping em    = Optional.ofNullable(mappingConfig.edges().get(edgeLabel))
                            .orElseThrow(() -> new IllegalArgumentException("No edge mapping found for label: " + edgeLabel));
                    String anchorCol  = GremlinToken.OUT.equals(direction) ? em.outColumn() : em.inColumn();
                    String targetCol  = GremlinToken.OUT.equals(direction) ? em.inColumn() : em.outColumn();
                    VertexMapping targetVm = resolver.resolveTargetVertexMapping(em, GremlinToken.OUT.equals(direction));
                    if (targetVm == null) {
                        selectJoiner.add(SqlFragment.OPEN_PAREN + SqlKeyword.SELECT_COUNT + em.table()
                                + SqlKeyword.WHERE + anchorCol + SqlFragment.SPACE_EQ_SPACE
                                + SqlFragment.PREFIX_V + vertexMapping.idColumn()
                                + SqlFragment.CLOSE_PAREN + SqlKeyword.AS + alias);
                    } else {
                        StringBuilder subq = new StringBuilder(SqlFragment.OPEN_PAREN)
                                .append(SqlKeyword.SELECT_COUNT)
                                .append(em.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_SUB_E)
                                .append(SqlKeyword.JOIN).append(targetVm.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_SUB_TV)
                                .append(SqlKeyword.ON).append(SqlFragment.ALIAS_SUB_TV).append(SqlFragment.DOT).append(targetVm.idColumn())
                                .append(SqlFragment.SPACE_EQ_SPACE).append(SqlFragment.PREFIX_E).append(targetCol)
                                .append(SqlKeyword.WHERE).append(SqlFragment.PREFIX_E).append(anchorCol)
                                .append(SqlFragment.SPACE_EQ_SPACE).append(SqlFragment.PREFIX_V).append(vertexMapping.idColumn());
                        for (int i = 2; i < parts.length; i++) {
                            int eq = parts[i].indexOf(SqlFragment.EQUALS_SIGN);
                            if (eq >= 1) {
                                String filterProp = parts[i].substring(0, eq);
                                String filterVal  = parts[i].substring(eq + 1);
                                subq.append(SqlKeyword.AND).append(SqlFragment.ALIAS_SUB_TV).append(SqlFragment.DOT)
                                        .append(resolver.mapVertexProperty(targetVm, filterProp))
                                        .append(SqlFragment.SPACE_EQ_SPACE)
                                        .append(SqlFragment.SQL_QUOTE)
                                        .append(filterVal.replace(SqlFragment.SQL_QUOTE,
                                                SqlFragment.SQL_QUOTE + SqlFragment.SQL_QUOTE))
                                        .append(SqlFragment.SQL_QUOTE);
                            }
                        }
                        subq.append(SqlFragment.CLOSE_PAREN);
                        selectJoiner.add(subq + SqlKeyword.AS + alias);
                    }
                }
                case OUT_NEIGHBOR_PROPERTY, IN_NEIGHBOR_PROPERTY -> {
                    String[] parts = projection.property().split(SqlFragment.COLON, 3);
                    if (parts.length < 3)
                        throw new IllegalArgumentException("Invalid NEIGHBOR_PROPERTY descriptor: " + projection.property());
                    boolean isOut    = GremlinToken.OUT.equals(parts[0]);
                    String edgeLabel = parts[1];
                    String neighborProp = parts[2];
                    EdgeMapping em   = Optional.ofNullable(mappingConfig.edges().get(edgeLabel))
                            .orElseThrow(() -> new IllegalArgumentException("No edge mapping found for label: " + edgeLabel));
                    VertexMapping neighborMapping = resolver.resolveTargetVertexMapping(em, isOut);
                    if (neighborMapping == null || !neighborMapping.properties().containsKey(neighborProp)) {
                        VertexMapping propMatched = resolver.resolveUniqueVertexMappingByProperty(neighborProp);
                        if (propMatched != null) neighborMapping = propMatched;
                    }
                    if (neighborMapping == null) neighborMapping = vertexMapping;
                    String anchorCol = isOut ? em.outColumn() : em.inColumn();
                    String targetCol = isOut ? em.inColumn() : em.outColumn();
                    String mappedCol = resolver.mapVertexProperty(neighborMapping, neighborProp);
                    String subqAlias = SqlFragment.ALIAS_NJE_PREFIX + neighborJoinIdx;
                    String vAlias    = SqlFragment.ALIAS_NJV_PREFIX + neighborJoinIdx++;
                    String aggExpr = buildNeighborAggSubquery(subqAlias, vAlias, mappedCol, em, neighborMapping,
                            anchorCol, targetCol, vertexMapping);
                    selectJoiner.add(aggExpr + SqlKeyword.AS + alias);
                }
                default ->
                    throw new IllegalArgumentException("Unsupported projection kind: " + projection.kind());
            }
            projIdx++;
        }

        // ── Base SELECT ... FROM v  LEFT JOIN (SELECT ... GROUP BY ... LIMIT MAX) ... ──
        StringBuilder baseSql = new StringBuilder(parsed.dedupRequested() ? SqlKeyword.SELECT_DISTINCT : SqlKeyword.SELECT)
                .append(selectJoiner)
                .append(SqlKeyword.FROM).append(vertexMapping.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_V);

        // Append LEFT JOINs — each degree aggregate is an inline derived table.
        // H2's query planner can re-correlate a GROUP BY subquery back into a per-row
        // scan if there is no LIMIT, turning an O(1) hash-aggregate into O(N).
        // Adding LIMIT 2147483647 prevents this on H2.
        // Trino/Iceberg does NOT support LIMIT inside a derived-table subquery in a
        // FROM/JOIN clause — the statement is rejected with "Schema must be specified
        // when session schema is not set".  We omit the LIMIT for Trino.
        boolean needsDerivedLimit = helper.dialect().requiresDerivedTableLimit();
        for (DegCte cte : degCtesOrdered.values()) {
            baseSql.append(SqlKeyword.LEFT_JOIN)
                   .append(SqlFragment.OPEN_PAREN)
                   .append(SqlKeyword.SELECT).append(cte.joinCol()).append(SqlKeyword.AS).append("_id")
                   .append(SqlFragment.COMMA_SPACE).append("COUNT(*) AS _cnt")
                   .append(SqlKeyword.FROM).append(cte.edgeTable());
            if (!cte.filterSql().isEmpty()) {
                // filterSql already starts with " AND "; replace leading " AND " with " WHERE "
                baseSql.append(SqlKeyword.WHERE).append(cte.filterSql().substring(SqlKeyword.AND.length()));
            }
            baseSql.append(" GROUP BY ").append(cte.joinCol());
            if (needsDerivedLimit) {
                baseSql.append(SqlKeyword.LIMIT).append(Integer.MAX_VALUE);
            }
            baseSql.append(SqlFragment.CLOSE_PAREN).append(SqlFragment.SPACE).append(cte.cteName())
                   .append(SqlKeyword.ON).append(cte.cteName()).append("._id")
                   .append(SqlFragment.SPACE_EQ_SPACE).append(SqlFragment.PREFIX_V).append(vertexMapping.idColumn());
        }

        whereBuilder.appendWhereClauseForVertexAlias(baseSql, params, parsed.filters(), vertexMapping, SqlFragment.ALIAS_V);
        if (parsed.whereClause() != null && whereBuilder.isStructuredWherePredicate(parsed.whereClause())) {
            whereBuilder.appendStructuredWherePredicate(
                    baseSql, params, parsed.whereClause(), vertexMapping, SqlFragment.ALIAS_V, !parsed.filters().isEmpty());
        }

        Integer limit = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();

        if (parsed.whereClause() != null) {
            if (parsed.whereClause().kind() == com.graphqueryengine.query.translate.sql.model.WhereKind.PROJECT_GT
                    || parsed.whereClause().kind() == com.graphqueryengine.query.translate.sql.model.WhereKind.PROJECT_GTE) {
                String numericLiteral = helper.sanitizeNumericLiteral(parsed.whereClause().right());
                String operator = parsed.whereClause().kind() == com.graphqueryengine.query.translate.sql.model.WhereKind.PROJECT_GTE
                        ? SqlOperator.GTE : SqlOperator.GT;
                StringBuilder wrapped = new StringBuilder(SqlKeyword.SELECT + SqlKeyword.STAR).append(SqlKeyword.FROM_SUBQUERY)
                        .append(baseSql).append(SqlFragment.CLOSE_PAREN).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_P)
                        .append(SqlKeyword.WHERE).append(SqlFragment.ALIAS_P).append(SqlFragment.DOT)
                        .append(helper.quoteAlias(parsed.whereClause().left()))
                        .append(SqlFragment.SPACE).append(operator).append(SqlFragment.SPACE).append(numericLiteral);
                helper.appendOrderBy(wrapped, parsed.orderByProperty(), parsed.orderDirection());
                SqlRenderHelper.appendLimit(wrapped, limit);
                return new TranslationResult(wrapped.toString(), params);
            }
            if (!whereBuilder.isStructuredWherePredicate(parsed.whereClause())) {
                throw new IllegalArgumentException(
                        "Only where(select('alias').is(gt/gte(n))) or where(outE/inE/bothE(...)) or where(out/in('label').has(...)) is supported with project(...)");
            }
        }

        appendProjectionOrderBy(baseSql, parsed, vertexMapping);
        SqlRenderHelper.appendLimit(baseSql, limit);
        return new TranslationResult(baseSql.toString(), params);
    }

    /**
     * Appends an ORDER BY clause for a projection query.
     *
     * <p>For properties that map directly to a vertex column (e.g. {@code accountId} →
     * {@code account_id}) we emit {@code ORDER BY v.account_id} — using the fully-qualified
     * column expression avoids the Trino/Iceberg ambiguity where a double-quoted alias in
     * {@code ORDER BY "accountId"} is resolved as a case-sensitive column name lookup against
     * the underlying table rather than the SELECT output alias, which causes the results to
     * appear unsorted.
     *
     * <p>For synthetic/computed projection aliases (e.g. degree CTEs like {@code suspiciousOut},
     * or {@code CHOOSE}-based buckets like {@code riskBucket}) that have no direct column
     * mapping, we fall back to the quoted alias — these are derived expressions and the alias
     * is the only handle available.
     */
    private void appendProjectionOrderBy(StringBuilder sql, ParsedTraversal parsed,
                                         VertexMapping vertexMapping) {
        String orderByProperty = parsed.orderByProperty();
        if (orderByProperty == null) return;
        String direction = parsed.orderDirection() != null ? parsed.orderDirection() : com.graphqueryengine.query.translate.sql.constant.SqlKeyword.ASC;
        // Try to resolve the property to a mapped column; if not found use the alias.
        String mappedColumn = vertexMapping.properties().get(orderByProperty);
        if (mappedColumn != null) {
            // Use the fully-qualified column expression to avoid alias-resolution ambiguity in Trino.
            sql.append(com.graphqueryengine.query.translate.sql.constant.SqlKeyword.ORDER_BY)
               .append(SqlFragment.PREFIX_V).append(mappedColumn)
               .append(SqlFragment.SPACE).append(direction);
        } else {
            // Derived/computed alias (degree CTE, CASE WHEN, etc.) — fall back to quoted alias.
            helper.appendOrderBy(sql, orderByProperty, parsed.orderDirection());
        }
    }

    /**
     * Builds the inline derived-table SQL fragment for a single degree aggregation.
     * The {@code LIMIT 2147483647} is included only when {@code withLimit=true} (H2 mode):
     * it prevents H2's predicate-pushdown from re-correlating the GROUP BY subquery back
     * into a per-row scan.  Trino/Iceberg rejects LIMIT inside a derived-table subquery.
     */
    @SuppressWarnings("unused") // kept for reference; logic is now inlined in buildProjectionSql
    private static String buildDegSubquery(DegCte cte, boolean withLimit) {
        StringBuilder sb = new StringBuilder(SqlFragment.OPEN_PAREN)
                .append(SqlKeyword.SELECT).append(cte.joinCol()).append(SqlKeyword.AS).append("_id")
                .append(", COUNT(*) AS _cnt")
                .append(SqlKeyword.FROM).append(cte.edgeTable());
        if (!cte.filterSql().isEmpty()) {
            sb.append(SqlKeyword.WHERE).append(cte.filterSql().substring(SqlKeyword.AND.length()));
        }
        sb.append(" GROUP BY ").append(cte.joinCol());
        if (withLimit) {
            sb.append(SqlKeyword.LIMIT).append(Integer.MAX_VALUE);
        }
        sb.append(SqlFragment.CLOSE_PAREN);
        return sb.toString();
    }

    private String buildNeighborAggSubquery(String subqAlias, String vAlias, String mappedCol,
                                             EdgeMapping em, VertexMapping neighborMapping,
                                             String anchorCol, String targetCol,
                                             VertexMapping vertexMapping) {
        return SqlFragment.OPEN_PAREN + SqlKeyword.SELECT + helper.buildNeighborAggExpr(vAlias + SqlFragment.DOT + mappedCol)
                + SqlKeyword.FROM + em.table() + SqlFragment.SPACE + subqAlias
                + SqlKeyword.JOIN + neighborMapping.table() + SqlFragment.SPACE + vAlias
                + SqlKeyword.ON + vAlias + SqlFragment.DOT + neighborMapping.idColumn() + SqlFragment.SPACE_EQ_SPACE + subqAlias + SqlFragment.DOT + targetCol
                + SqlKeyword.WHERE + subqAlias + SqlFragment.DOT + anchorCol + SqlFragment.SPACE_EQ_SPACE + SqlFragment.PREFIX_V + vertexMapping.idColumn() + SqlFragment.CLOSE_PAREN;
    }
}
