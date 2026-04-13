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
import com.graphqueryengine.query.translate.sql.where.WhereClauseBuilder;

import java.util.ArrayList;
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

    public TranslationResult buildProjectionSql(ParsedTraversal parsed, MappingConfig mappingConfig,
                                                 VertexMapping vertexMapping) {
        if (parsed.countRequested())
            throw new IllegalArgumentException("count() cannot be combined with project(...)");
        if (parsed.valueProperty() != null)
            throw new IllegalArgumentException("values() cannot be combined with project(...)");

        List<Object> params = new ArrayList<>();
        StringJoiner selectJoiner = new StringJoiner(SqlFragment.COMMA_SPACE);
        int neighborJoinIdx = 0;

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
                    selectJoiner.add(SqlKeyword.CASE_WHEN + SqlFragment.PREFIX_V + column + SqlFragment.SPACE + operator + SqlFragment.SPACE + threshold
                            + SqlKeyword.THEN + whenTrue + SqlKeyword.ELSE + whenFalse + SqlKeyword.END + SqlKeyword.AS + alias);
                }
                case EDGE_DEGREE -> {
                    String[] parts = projection.property().split(SqlFragment.COLON, -1);
                    if (parts.length < 2)
                        throw new IllegalArgumentException("Invalid EDGE_DEGREE property: " + projection.property());
                    String direction  = parts[0];
                    String edgeLabel  = parts[1];
                    EdgeMapping em    = Optional.ofNullable(mappingConfig.edges().get(edgeLabel))
                            .orElseThrow(() -> new IllegalArgumentException("No edge mapping found for label: " + edgeLabel));
                    String joinColumn = GremlinToken.OUT.equals(direction) ? em.outColumn() : em.inColumn();
                    StringBuilder subq = new StringBuilder(SqlFragment.OPEN_PAREN)
                            .append(SqlKeyword.SELECT_COUNT)
                            .append(em.table()).append(SqlKeyword.WHERE).append(joinColumn)
                            .append(SqlFragment.SPACE_EQ_SPACE).append(SqlFragment.PREFIX_V).append(vertexMapping.idColumn());
                    for (int i = 2; i < parts.length; i++) {
                        int eq = parts[i].indexOf(SqlFragment.EQUALS_SIGN);
                        if (eq >= 1) {
                            String filterProp = parts[i].substring(0, eq);
                            String filterVal  = parts[i].substring(eq + 1);
                            subq.append(SqlKeyword.AND).append(resolver.mapEdgeProperty(em, filterProp))
                                    .append(SqlFragment.SPACE_EQ_SPACE).append(SqlFragment.SQL_QUOTE).append(filterVal.replace(SqlFragment.SQL_QUOTE, SqlFragment.SQL_QUOTE + SqlFragment.SQL_QUOTE)).append(SqlFragment.SQL_QUOTE);
                        }
                    }
                    subq.append(SqlFragment.CLOSE_PAREN);
                    selectJoiner.add(subq + SqlKeyword.AS + alias);
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
                        selectJoiner.add(SqlFragment.OPEN_PAREN + SqlKeyword.SELECT_COUNT + em.table() + SqlKeyword.WHERE + anchorCol
                                + SqlFragment.SPACE_EQ_SPACE + SqlFragment.PREFIX_V + vertexMapping.idColumn() + SqlFragment.CLOSE_PAREN + SqlKeyword.AS + alias);
                    } else {
                        StringBuilder subq = new StringBuilder(SqlFragment.OPEN_PAREN)
                                .append(SqlKeyword.SELECT_COUNT)
                                .append(em.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_SUB_E)
                                .append(SqlKeyword.JOIN).append(targetVm.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_SUB_TV)
                                .append(SqlKeyword.ON).append(SqlFragment.ALIAS_SUB_TV).append(SqlFragment.DOT).append(targetVm.idColumn()).append(SqlFragment.SPACE_EQ_SPACE).append(SqlFragment.PREFIX_E).append(targetCol)
                                .append(SqlKeyword.WHERE).append(SqlFragment.PREFIX_E).append(anchorCol).append(SqlFragment.SPACE_EQ_SPACE).append(SqlFragment.PREFIX_V).append(vertexMapping.idColumn());
                        for (int i = 2; i < parts.length; i++) {
                            int eq = parts[i].indexOf(SqlFragment.EQUALS_SIGN);
                            if (eq >= 1) {
                                String filterProp = parts[i].substring(0, eq);
                                String filterVal  = parts[i].substring(eq + 1);
                                subq.append(SqlKeyword.AND).append(SqlFragment.ALIAS_SUB_TV).append(SqlFragment.DOT).append(resolver.mapVertexProperty(targetVm, filterProp))
                                        .append(SqlFragment.SPACE_EQ_SPACE).append(SqlFragment.SQL_QUOTE).append(filterVal.replace(SqlFragment.SQL_QUOTE, SqlFragment.SQL_QUOTE + SqlFragment.SQL_QUOTE)).append(SqlFragment.SQL_QUOTE);
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
        }

        StringBuilder baseSql = new StringBuilder(parsed.dedupRequested() ? SqlKeyword.SELECT_DISTINCT : SqlKeyword.SELECT)
                .append(selectJoiner).append(SqlKeyword.FROM).append(vertexMapping.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_V);
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
                String operator = parsed.whereClause().kind() == com.graphqueryengine.query.translate.sql.model.WhereKind.PROJECT_GTE ? SqlOperator.GTE : SqlOperator.GT;
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
        helper.appendOrderBy(baseSql, parsed.orderByProperty(), parsed.orderDirection());
        SqlRenderHelper.appendLimit(baseSql, limit);
        return new TranslationResult(baseSql.toString(), params);
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

