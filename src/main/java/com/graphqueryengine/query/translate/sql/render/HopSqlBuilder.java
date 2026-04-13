package com.graphqueryengine.query.translate.sql.render;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.translate.sql.mapping.SqlMappingResolver;
import com.graphqueryengine.query.translate.sql.model.HopStep;
import com.graphqueryengine.query.translate.sql.model.ParsedTraversal;
import com.graphqueryengine.query.translate.sql.model.ProjectionField;
import com.graphqueryengine.query.translate.sql.model.ProjectionKind;
import com.graphqueryengine.query.translate.sql.model.WhereKind;
import com.graphqueryengine.query.translate.sql.where.WhereClauseBuilder;
import com.graphqueryengine.query.translate.sql.constant.GremlinToken;
import com.graphqueryengine.query.translate.sql.constant.SqlFragment;
import com.graphqueryengine.query.translate.sql.constant.SqlKeyword;
import com.graphqueryengine.query.translate.sql.dialect.SqlDialect;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * Builds SQL for multi-hop graph traversals ({@code g.V().out(...).out(...)...}).
 *
 * <p>Handles three structural cases:
 * <ol>
 *   <li>Single-direction, single-label hops (the common case)</li>
 *   <li>Multi-label hops ({@code out('A','B')}) — expanded via UNION</li>
 *   <li>{@code both()} hops — expanded into an out-branch UNION an in-branch</li>
 * </ol>
 */
public class HopSqlBuilder {

    private final SqlMappingResolver resolver;
    private final WhereClauseBuilder whereBuilder;
    private final SqlRenderHelper helper;
    private final SqlDialect dialect;

    public HopSqlBuilder(SqlMappingResolver resolver, WhereClauseBuilder whereBuilder,
                          SqlRenderHelper helper, SqlDialect dialect) {
        this.resolver     = resolver;
        this.whereBuilder = whereBuilder;
        this.helper       = helper;
        this.dialect      = dialect;
    }

    /** Legacy constructor — assumes CTE support (H2 / standard SQL). */
    public HopSqlBuilder(SqlMappingResolver resolver, WhereClauseBuilder whereBuilder,
                          SqlRenderHelper helper) {
        this(resolver, whereBuilder, helper, new com.graphqueryengine.query.translate.sql.dialect.StandardSqlDialect());
    }

    // ── Entry point ───────────────────────────────────────────────────────────

    public TranslationResult build(ParsedTraversal parsed,
                                   VertexMapping startVertexMapping) {
        if (parsed.hops().stream().anyMatch(h -> GremlinToken.BOTH.equals(h.direction())))
            return buildBothHopUnionSql(parsed, startVertexMapping);
        if (parsed.hops().stream().anyMatch(h -> h.labels().size() > 1))
            return buildMultiLabelHopUnionSql(parsed, startVertexMapping);
        return buildStandardHopSql(parsed, startVertexMapping);
    }

    // ── Standard single-label hop SQL ─────────────────────────────────────────

    @SuppressWarnings("DuplicatedCode")
    private TranslationResult buildStandardHopSql(ParsedTraversal parsed,
                                                  VertexMapping startVertexMapping) {
        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        int hopCount = parsed.hops().size();
        HopStep terminalHop = parsed.hops().get(hopCount - 1);
        boolean terminalEdgeHop = isEdgeHop(terminalHop.direction());
        String finalVertexAlias = SqlFragment.V_PREFIX + hopCount;
        String finalEdgeAlias   = SqlFragment.E_PREFIX + hopCount;
        String idCol = startVertexMapping.idColumn();
        EdgeMapping finalEdgeMapping = terminalEdgeHop
                ? resolver.resolveEdgeMapping(terminalHop.singleLabel()) : null;

        // SELECT clause
        if (parsed.countRequested()) {
                if (parsed.dedupRequested()) {
                String da = terminalEdgeHop ? finalEdgeAlias : finalVertexAlias;
                String dc = terminalEdgeHop ? finalEdgeMapping.idColumn() : idCol;
                sql.append(SqlKeyword.SELECT + SqlKeyword.COUNT_DISTINCT).append(da).append(SqlFragment.DOT).append(dc).append(SqlFragment.CLOSE_PAREN).append(SqlKeyword.AS).append(SqlFragment.ALIAS_COUNT);
            } else {
                sql.append(SqlKeyword.SELECT).append(SqlKeyword.COUNT_STAR);
            }
        } else if (parsed.sumRequested()) {
            sql.append(SqlKeyword.SELECT).append(helper.buildHopSumExpression(parsed, startVertexMapping, finalVertexAlias));
        } else if (parsed.meanRequested()) {
            sql.append(SqlKeyword.SELECT).append(helper.buildHopMeanExpression(parsed, startVertexMapping, finalVertexAlias));
        } else if (!parsed.projections().isEmpty()) {
            VertexMapping finalVertexMapping = terminalEdgeHop
                    ? null
                    : resolver.resolveFinalHopVertexMapping(parsed.hops(), startVertexMapping);
            StringJoiner projectionSelect = new StringJoiner(SqlFragment.COMMA_SPACE);
            for (ProjectionField projection : parsed.projections()) {
                if (projection.kind() == ProjectionKind.IDENTITY) {
                    String a = terminalEdgeHop ? finalEdgeAlias : finalVertexAlias;
                    String c = terminalEdgeHop ? finalEdgeMapping.idColumn() : idCol;
                    projectionSelect.add(a + SqlFragment.DOT + c + SqlKeyword.AS + helper.quoteAlias(projection.alias()));
                    continue;
                }
                if (projection.kind() != ProjectionKind.EDGE_PROPERTY)
                    throw new IllegalArgumentException(
                            "Hop projection currently supports by('property') or by(identity()) only");
                String mappedColumn = terminalEdgeHop
                        ? resolver.mapEdgeProperty(finalEdgeMapping, projection.property())
                        : resolver.mapVertexProperty(finalVertexMapping, projection.property());
                String a = terminalEdgeHop ? finalEdgeAlias : finalVertexAlias;
                projectionSelect.add(a + SqlFragment.DOT + mappedColumn + SqlKeyword.AS + helper.quoteAlias(projection.alias()));
            }
            sql.append(parsed.dedupRequested() ? SqlKeyword.SELECT_DISTINCT : SqlKeyword.SELECT).append(projectionSelect);
        } else if (parsed.pathSeen() && !parsed.pathByProperties().isEmpty()) {
            sql.append(helper.buildPathSelectClause(
                    parsed.pathByProperties(), hopCount, startVertexMapping, parsed.hops()));
        } else if (parsed.valueMapRequested()) {
            if (terminalEdgeHop) {
                sql.append(parsed.dedupRequested() ? SqlKeyword.SELECT_DISTINCT : SqlKeyword.SELECT)
                        .append(helper.buildAliasValueMapSelectForEdge(finalEdgeMapping, finalEdgeAlias));
            } else {
                VertexMapping finalVm = resolver.resolveFinalHopVertexMapping(parsed.hops(), startVertexMapping);
                sql.append(parsed.dedupRequested() ? SqlKeyword.SELECT_DISTINCT : SqlKeyword.SELECT)
                        .append(helper.buildAliasValueMapSelectForVertex(finalVm, finalVertexAlias, parsed.valueMapKeys()));
            }
        } else if (parsed.valueProperty() != null) {
            if (terminalEdgeHop) {
                String mc = resolver.mapEdgeProperty(finalEdgeMapping, parsed.valueProperty());
                sql.append(SqlKeyword.SELECT).append(parsed.dedupRequested() ? SqlKeyword.DISTINCT : "")
                        .append(finalEdgeAlias).append(SqlFragment.DOT).append(mc).append(SqlKeyword.AS).append(parsed.valueProperty());
            } else {
                VertexMapping finalVm = resolver.resolveFinalHopVertexMapping(parsed.hops(), startVertexMapping);
                String mc = resolver.mapVertexProperty(finalVm, parsed.valueProperty());
                sql.append(SqlKeyword.SELECT).append(parsed.dedupRequested() ? SqlKeyword.DISTINCT : "")
                        .append(finalVertexAlias).append(SqlFragment.DOT).append(mc).append(SqlKeyword.AS).append(parsed.valueProperty());
            }
        } else {
            sql.append(parsed.dedupRequested() ? SqlKeyword.SELECT_DISTINCT : SqlKeyword.SELECT)
                    .append(terminalEdgeHop ? finalEdgeAlias : finalVertexAlias).append(SqlFragment.DOT).append(SqlFragment.STAR);
        }

        // FROM clause (with optional pre-hop LIMIT subquery)
        appendFromClause(sql, params, parsed, startVertexMapping);

        // JOINs
        for (int i = 0; i < hopCount; i++) {
            HopStep hop = parsed.hops().get(i);
            EdgeMapping edgeMapping = resolver.resolveEdgeMapping(hop.singleLabel());
            String edgeAlias       = SqlFragment.E_PREFIX + (i + 1);
            String fromVertexAlias = SqlFragment.V_PREFIX + i;
            String toVertexAlias   = SqlFragment.V_PREFIX + (i + 1);
            String fromId  = fromVertexAlias + SqlFragment.DOT + idCol;
            String toId    = toVertexAlias + SqlFragment.DOT + idCol;
            String edgeOut = edgeAlias + SqlFragment.DOT + edgeMapping.outColumn();
            String edgeIn  = edgeAlias + SqlFragment.DOT + edgeMapping.inColumn();
            sql.append(SqlKeyword.JOIN).append(edgeMapping.table()).append(SqlFragment.SPACE).append(edgeAlias);
            if (GremlinToken.BOTH_E.equals(hop.direction())) {
                sql.append(SqlKeyword.ON).append(SqlFragment.OPEN_PAREN).append(edgeOut).append(SqlFragment.SPACE_EQ_SPACE).append(fromId)
                        .append(SqlKeyword.OR).append(edgeIn).append(SqlFragment.SPACE_EQ_SPACE).append(fromId).append(SqlFragment.CLOSE_PAREN);
            } else if (GremlinToken.OUT.equals(hop.direction()) || GremlinToken.OUT_E.equals(hop.direction())) {
                sql.append(SqlKeyword.ON).append(edgeOut).append(SqlFragment.SPACE_EQ_SPACE).append(fromId);
            } else {
                sql.append(SqlKeyword.ON).append(edgeIn).append(SqlFragment.SPACE_EQ_SPACE).append(fromId);
            }

            if (!isEdgeHop(hop.direction())) {
                VertexMapping toVm = resolver.resolveHopTargetVertexMapping(
                        edgeMapping, GremlinToken.OUT.equals(hop.direction()), startVertexMapping);
                sql.append(SqlKeyword.JOIN).append(toVm.table()).append(SqlFragment.SPACE).append(toVertexAlias);
                sql.append(GremlinToken.OUT.equals(hop.direction())
                        ? SqlKeyword.ON + toId + SqlFragment.SPACE_EQ_SPACE + edgeIn
                        : SqlKeyword.ON + toId + SqlFragment.SPACE_EQ_SPACE + edgeOut);
            }
        }

        // WHERE
        boolean hasWhere = false;
        if (parsed.preHopLimit() == null) {
            if (!parsed.filters().isEmpty()) {
                VertexMapping finalVm = resolver.resolveFinalHopVertexMapping(parsed.hops(), startVertexMapping);
                hasWhere = whereBuilder.appendHopFiltersWithTargetFallback(
                        sql, params, parsed.filters(), startVertexMapping, finalVertexAlias, finalVm, parsed);
            }
            if (parsed.whereClause() != null) {
                if (whereBuilder.isStructuredWherePredicate(parsed.whereClause())) {
                    whereBuilder.appendStructuredWherePredicate(
                            sql, params, parsed.whereClause(), startVertexMapping, SqlFragment.V0, hasWhere);
                } else if (parsed.whereClause().kind() == WhereKind.EQ_ALIAS) {
                    whereBuilder.appendEqAliasPredicate(
                            sql, parsed.whereClause(), parsed.asAliases(), finalVertexAlias, idCol, hasWhere);
                } else if (parsed.whereClause().kind() == WhereKind.NEQ_ALIAS) {
                    whereBuilder.appendNeqAliasPredicate(
                            sql, parsed.whereClause(), parsed.asAliases(), parsed.whereByProperty(),
                            parsed.hops(), startVertexMapping, idCol, hasWhere);
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported where() predicate for hop traversal: " + parsed.whereClause().kind());
                }
                hasWhere = true;
            }
        }

        return getTranslationResult(parsed, params, sql, hopCount, idCol, hasWhere);
    }

    private TranslationResult getTranslationResult(ParsedTraversal parsed, List<Object> params, StringBuilder sql, int hopCount, String idCol, boolean hasWhere) {
        if (parsed.simplePathRequested() && hopCount > 0) {
            StringJoiner cc = SqlRenderHelper.buildSimplePathConditions(hopCount, idCol);
            sql.append(hasWhere ? SqlKeyword.AND : SqlKeyword.WHERE).append(cc);
        }

        SqlRenderHelper.appendLimit(sql, parsed.limit());
        return new TranslationResult(sql.toString(), params);
    }

    // ── Multi-label hop (UNION) ───────────────────────────────────────────────

    @SuppressWarnings("DuplicatedCode")
    private TranslationResult buildMultiLabelHopUnionSql(ParsedTraversal parsed,
                                                         VertexMapping startVertexMapping) {
        int hopCount = parsed.hops().size();
        if (hopCount == 1) {
            HopStep hop = parsed.hops().get(0);
            List<String> branchSqls = new ArrayList<>();
            List<Object> allParams  = new ArrayList<>();
            for (String label : hop.labels()) {
                ParsedTraversal singleParsed = parsed.withHops(List.of(new HopStep(hop.direction(), label)));
                TranslationResult br = buildStandardHopSql(singleParsed, startVertexMapping);
                branchSqls.add(br.sql());
                allParams.addAll(br.parameters());
            }
            String unionKeyword = parsed.dedupRequested() ? SqlKeyword.UNION : SqlKeyword.UNION_ALL;
            StringBuilder finalSql = new StringBuilder(String.join(unionKeyword, branchSqls));
            SqlRenderHelper.appendLimit(finalSql, parsed.limit());
            return new TranslationResult(finalSql.toString(), allParams);
        }

        // Multi-hop multi-label: primary + alt LEFT JOINs per hop
        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        String idCol = startVertexMapping.idColumn();
        String finalVertexAlias = SqlFragment.V_PREFIX + hopCount;

        if (parsed.countRequested()) {
            sql.append(parsed.dedupRequested()
                    ? SqlKeyword.SELECT + SqlKeyword.COUNT_DISTINCT + finalVertexAlias + SqlFragment.DOT + idCol + SqlFragment.CLOSE_PAREN + SqlKeyword.AS + SqlFragment.ALIAS_COUNT
                    : SqlKeyword.SELECT + SqlKeyword.COUNT_STAR);
        } else if (!parsed.projections().isEmpty()) {
            VertexMapping finalVm = startVertexMapping;
            for (HopStep hop : parsed.hops()) {
                if (!hop.labels().isEmpty()) {
                    EdgeMapping em = resolver.resolveEdgeMapping(hop.labels().get(0));
                    finalVm = resolver.resolveHopTargetVertexMapping(em, GremlinToken.OUT.equals(hop.direction()), finalVm);
                }
            }
            StringJoiner pj = new StringJoiner(SqlFragment.COMMA_SPACE);
            for (ProjectionField pf : parsed.projections()) {
                if (pf.kind() != ProjectionKind.EDGE_PROPERTY)
                    throw new IllegalArgumentException("Hop projection currently supports by('property') only");
                pj.add(finalVertexAlias + SqlFragment.DOT + resolver.mapVertexProperty(finalVm, pf.property())
                        + SqlKeyword.AS + helper.quoteAlias(pf.alias()));
            }
            sql.append(parsed.dedupRequested() ? SqlKeyword.SELECT_DISTINCT : SqlKeyword.SELECT).append(pj);
        } else if (parsed.pathSeen() && !parsed.pathByProperties().isEmpty()) {
            sql.append(helper.buildPathSelectClause(
                    parsed.pathByProperties(), hopCount, startVertexMapping, parsed.hops()));
        } else if (parsed.valueProperty() != null) {
            // ...existing code for value property...
            VertexMapping lastVm = startVertexMapping;
            for (HopStep hop : parsed.hops()) {
                if (!hop.labels().isEmpty()) {
                    EdgeMapping em = resolver.resolveEdgeMapping(hop.labels().get(0));
                    lastVm = resolver.resolveHopTargetVertexMapping(em, GremlinToken.OUT.equals(hop.direction()), lastVm);
                }
            }
            String col = lastVm.properties().get(parsed.valueProperty());
            if (col == null) {
                for (VertexMapping vm : resolver.mappingConfig().vertices().values()) {
                    if (vm.properties().containsKey(parsed.valueProperty())) {
                        col = vm.properties().get(parsed.valueProperty());
                        break;
                    }
                }
            }
            if (col == null)
                throw new IllegalArgumentException("No vertex property mapping found for property: " + parsed.valueProperty());
            sql.append(SqlKeyword.SELECT).append(parsed.dedupRequested() ? SqlKeyword.DISTINCT : "")
                    .append(finalVertexAlias).append(SqlFragment.DOT).append(col).append(SqlKeyword.AS).append(parsed.valueProperty());
        } else {
            sql.append(parsed.dedupRequested() ? SqlKeyword.SELECT_DISTINCT : SqlKeyword.SELECT).append(finalVertexAlias).append(SqlFragment.DOT).append(SqlFragment.STAR);
        }

        appendFromClause(sql, params, parsed, startVertexMapping);

        VertexMapping currentSource = startVertexMapping;
        for (int i = 0; i < hopCount; i++) {
            HopStep hop     = parsed.hops().get(i);
            List<String> ls = hop.labels();
            String dir      = hop.direction();
            String toAlias  = SqlFragment.V_PREFIX + (i + 1);
            String fromId   = SqlFragment.V_PREFIX + i + SqlFragment.DOT + idCol;
            String toId     = toAlias + SqlFragment.DOT + idCol;
            boolean outDir  = GremlinToken.OUT.equals(dir);
            EdgeMapping primaryEdge = resolver.resolveEdgeMapping(ls.get(0));
            VertexMapping primaryTarget = resolver.resolveHopTargetVertexMapping(primaryEdge, outDir, currentSource);
            String eAlias = SqlFragment.E_PREFIX + (i + 1);
            sql.append(SqlKeyword.JOIN).append(primaryEdge.table()).append(SqlFragment.SPACE).append(eAlias)
                    .append(SqlKeyword.ON).append(eAlias).append(SqlFragment.DOT)
                    .append(outDir ? primaryEdge.outColumn() : primaryEdge.inColumn())
                    .append(SqlFragment.SPACE_EQ_SPACE).append(fromId);
            sql.append(SqlKeyword.JOIN).append(primaryTarget.table()).append(SqlFragment.SPACE).append(toAlias)
                    .append(SqlKeyword.ON).append(toId).append(SqlFragment.SPACE_EQ_SPACE).append(eAlias).append(SqlFragment.DOT)
                    .append(outDir ? primaryEdge.inColumn() : primaryEdge.outColumn());
            for (int l = 1; l < ls.size(); l++) {
                EdgeMapping altEdge = resolver.resolveEdgeMapping(ls.get(l));
                VertexMapping altTarget = resolver.resolveHopTargetVertexMapping(altEdge, outDir, currentSource);
                String altEAlias = SqlFragment.E_PREFIX + (i + 1) + SqlFragment.UNDERSCORE + l;
                String altVAlias = SqlFragment.VT_PREFIX + (i + 1) + SqlFragment.UNDERSCORE + l;
                sql.append(SqlKeyword.LEFT_JOIN).append(altEdge.table()).append(SqlFragment.SPACE).append(altEAlias)
                        .append(SqlKeyword.ON).append(altEAlias).append(SqlFragment.DOT)
                        .append(outDir ? altEdge.outColumn() : altEdge.inColumn())
                        .append(SqlFragment.SPACE_EQ_SPACE).append(fromId);
                sql.append(SqlKeyword.LEFT_JOIN).append(altTarget.table()).append(SqlFragment.SPACE).append(altVAlias)
                        .append(SqlKeyword.ON).append(altVAlias).append(SqlFragment.DOT).append(altTarget.idColumn())
                        .append(SqlFragment.SPACE_EQ_SPACE).append(altEAlias).append(SqlFragment.DOT)
                        .append(outDir ? altEdge.inColumn() : altEdge.outColumn());
            }
            currentSource = primaryTarget;
        }

        boolean hasWhere = false;
        if (parsed.preHopLimit() == null) {
            if (!parsed.filters().isEmpty()) {
                whereBuilder.appendWhereClauseForVertexAlias(sql, params, parsed.filters(), startVertexMapping, SqlFragment.V0);
                hasWhere = true;
            }
            if (parsed.whereClause() != null && whereBuilder.isStructuredWherePredicate(parsed.whereClause())) {
                whereBuilder.appendStructuredWherePredicate(
                        sql, params, parsed.whereClause(), startVertexMapping, SqlFragment.V0, hasWhere);
                hasWhere = true;
            }
        }

        return getTranslationResult(parsed, params, sql, hopCount, idCol, hasWhere);
    }

    // ── both() hop (UNION of two branches) ───────────────────────────────────

    @SuppressWarnings("DuplicatedCode")
    private TranslationResult buildBothHopUnionSql(ParsedTraversal parsed,
                                                   VertexMapping startVertexMapping) {
        int hopCount = parsed.hops().size();
        String idCol = startVertexMapping.idColumn();
        String finalVertexAlias = SqlFragment.V_PREFIX + hopCount;

        String selectClause;
        if (parsed.countRequested()) {
            selectClause = finalVertexAlias + SqlFragment.DOT + idCol;
        } else if (!parsed.projections().isEmpty()) {
            VertexMapping finalVm = startVertexMapping;
            for (HopStep hop : parsed.hops()) {
                if (!hop.labels().isEmpty()) {
                    EdgeMapping em = resolver.resolveEdgeMapping(hop.labels().get(0));
                    finalVm = resolver.resolveHopTargetVertexMapping(em, GremlinToken.OUT.equals(hop.direction()), finalVm);
                }
            }
            StringJoiner pj = new StringJoiner(SqlFragment.COMMA_SPACE);
            for (ProjectionField projection : parsed.projections()) {
                if (projection.kind() != ProjectionKind.EDGE_PROPERTY)
                    throw new IllegalArgumentException("both() projection currently supports by('property') only");
                pj.add(finalVertexAlias + SqlFragment.DOT + resolver.mapVertexProperty(finalVm, projection.property())
                        + SqlKeyword.AS + helper.quoteAlias(projection.alias()));
            }
            selectClause = pj.toString();
        } else if (parsed.pathSeen() && !parsed.pathByProperties().isEmpty()) {
            selectClause = helper.buildPathSelectClause(
                    parsed.pathByProperties(), hopCount, startVertexMapping, parsed.hops())
                    .substring(SqlKeyword.SELECT.length());
        } else if (parsed.valueProperty() != null) {
            VertexMapping finalVm = startVertexMapping;
            for (HopStep hop : parsed.hops()) {
                if (!hop.labels().isEmpty()) {
                    EdgeMapping em = resolver.resolveEdgeMapping(hop.labels().get(0));
                    finalVm = resolver.resolveHopTargetVertexMapping(em, GremlinToken.OUT.equals(hop.direction()), finalVm);
                }
            }
            selectClause = finalVertexAlias + SqlFragment.DOT + resolver.mapVertexProperty(finalVm, parsed.valueProperty())
                    + SqlKeyword.AS + parsed.valueProperty();
        } else {
            selectClause = finalVertexAlias + SqlFragment.DOT + SqlFragment.STAR;
        }

        // Shared start FROM — use CTE when available to avoid correlated-subquery trap
        List<Object> sharedStartParams = new ArrayList<>();
        StringBuilder ctePreamble = new StringBuilder();
        String startFromRef;
        if (parsed.preHopLimit() != null) {
            StringBuilder inner = new StringBuilder()
                    .append(SqlKeyword.SELECT).append(SqlKeyword.STAR)
                    .append(SqlKeyword.FROM).append(startVertexMapping.table());
            whereBuilder.appendWhereClauseForVertex(inner, sharedStartParams, parsed.filters(), startVertexMapping);
            if (parsed.whereClause() != null && whereBuilder.isStructuredWherePredicate(parsed.whereClause())) {
                whereBuilder.appendStructuredWherePredicate(
                        inner, sharedStartParams, parsed.whereClause(), startVertexMapping, null,
                        !parsed.filters().isEmpty());
            }
            inner.append(SqlKeyword.LIMIT).append(parsed.preHopLimit());
            if (dialect.supportsCte()) {
                ctePreamble.append(SqlKeyword.WITH).append(SqlFragment.START_ALIAS)
                           .append(SqlKeyword.AS_OPEN).append(inner)
                           .append(SqlKeyword.CLOSE_PAREN).append(SqlFragment.SPACE);
                startFromRef = SqlFragment.START_ALIAS + SqlFragment.SPACE + SqlFragment.V0;
            } else {
                startFromRef = SqlFragment.OPEN_PAREN + inner + SqlFragment.CLOSE_PAREN + SqlFragment.SPACE + SqlFragment.V0;
            }
        } else {
            startFromRef = startVertexMapping.table() + SqlFragment.SPACE + SqlFragment.V0;
        }

        List<Object> outParams = new ArrayList<>();
        List<Object> inParams  = new ArrayList<>();
        StringBuilder outBranch = new StringBuilder(SqlKeyword.SELECT).append(selectClause).append(SqlKeyword.FROM).append(startFromRef);
        StringBuilder inBranch  = new StringBuilder(SqlKeyword.SELECT).append(selectClause).append(SqlKeyword.FROM).append(startFromRef);

        for (int i = 0; i < hopCount; i++) {
            HopStep hop = parsed.hops().get(i);
            EdgeMapping edgeMapping = resolver.resolveEdgeMapping(hop.singleLabel());
            String edgeAlias       = SqlFragment.E_PREFIX + (i + 1);
            String fromVertexAlias = SqlFragment.V_PREFIX + i;
            String toVertexAlias   = SqlFragment.V_PREFIX + (i + 1);
            String fromId = fromVertexAlias + SqlFragment.DOT + idCol;
            String toId   = toVertexAlias + SqlFragment.DOT + idCol;

            if (GremlinToken.BOTH.equals(hop.direction())) {
                outBranch.append(SqlKeyword.JOIN).append(edgeMapping.table()).append(SqlFragment.SPACE).append(edgeAlias)
                        .append(SqlKeyword.ON).append(edgeAlias).append(SqlFragment.DOT).append(edgeMapping.outColumn()).append(SqlFragment.SPACE_EQ_SPACE).append(fromId)
                        .append(SqlKeyword.JOIN).append(startVertexMapping.table()).append(SqlFragment.SPACE).append(toVertexAlias)
                        .append(SqlKeyword.ON).append(toId).append(SqlFragment.SPACE_EQ_SPACE).append(edgeAlias).append(SqlFragment.DOT).append(edgeMapping.inColumn());
                inBranch.append(SqlKeyword.JOIN).append(edgeMapping.table()).append(SqlFragment.SPACE).append(edgeAlias)
                        .append(SqlKeyword.ON).append(edgeAlias).append(SqlFragment.DOT).append(edgeMapping.inColumn()).append(SqlFragment.SPACE_EQ_SPACE).append(fromId)
                        .append(SqlKeyword.JOIN).append(startVertexMapping.table()).append(SqlFragment.SPACE).append(toVertexAlias)
                        .append(SqlKeyword.ON).append(toId).append(SqlFragment.SPACE_EQ_SPACE).append(edgeAlias).append(SqlFragment.DOT).append(edgeMapping.outColumn());
            } else {
                boolean outDir = GremlinToken.OUT.equals(hop.direction());
                String joinFrag = SqlKeyword.JOIN + edgeMapping.table() + SqlFragment.SPACE + edgeAlias
                        + SqlKeyword.ON + edgeAlias + SqlFragment.DOT + (outDir ? edgeMapping.outColumn() : edgeMapping.inColumn())
                        + SqlFragment.SPACE_EQ_SPACE + fromId
                        + SqlKeyword.JOIN + startVertexMapping.table() + SqlFragment.SPACE + toVertexAlias
                        + SqlKeyword.ON + toId + SqlFragment.SPACE_EQ_SPACE + edgeAlias + SqlFragment.DOT + (outDir ? edgeMapping.inColumn() : edgeMapping.outColumn());
                outBranch.append(joinFrag);
                inBranch.append(joinFrag);
            }
        }

        boolean branchHasWhere = false;
        if (parsed.preHopLimit() == null) {
            if (!parsed.filters().isEmpty()) {
                whereBuilder.appendWhereClauseForVertexAlias(outBranch, outParams, parsed.filters(), startVertexMapping, SqlFragment.V0);
                whereBuilder.appendWhereClauseForVertexAlias(inBranch, inParams, parsed.filters(), startVertexMapping, SqlFragment.V0);
                branchHasWhere = true;
            }
            if (parsed.whereClause() != null && whereBuilder.isStructuredWherePredicate(parsed.whereClause())) {
                whereBuilder.appendStructuredWherePredicate(outBranch, outParams, parsed.whereClause(), startVertexMapping, SqlFragment.V0, branchHasWhere);
                whereBuilder.appendStructuredWherePredicate(inBranch, inParams, parsed.whereClause(), startVertexMapping, SqlFragment.V0, branchHasWhere);
                branchHasWhere = true;
            }
        }

        if (parsed.simplePathRequested() && hopCount > 0) {
            StringJoiner cc = SqlRenderHelper.buildSimplePathConditions(hopCount, idCol);
            outBranch.append(branchHasWhere ? SqlKeyword.AND : SqlKeyword.WHERE).append(cc);
            inBranch.append(branchHasWhere ? SqlKeyword.AND : SqlKeyword.WHERE).append(cc);
        }

        String unionKeyword = parsed.dedupRequested() ? SqlKeyword.UNION : SqlKeyword.UNION_ALL;
        String unionSql = outBranch + unionKeyword + inBranch;

        StringBuilder finalSql;
        if (parsed.countRequested()) {
            finalSql = parsed.dedupRequested()
                    ? new StringBuilder(SqlKeyword.SELECT + SqlKeyword.COUNT_DISTINCT).append(idCol).append(SqlFragment.CLOSE_PAREN).append(SqlKeyword.AS).append(SqlFragment.ALIAS_COUNT).append(SqlKeyword.FROM_SUBQUERY).append(unionSql).append(SqlFragment.CLOSE_PAREN).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_U)
                    : new StringBuilder(SqlKeyword.SELECT + SqlKeyword.COUNT_STAR).append(SqlKeyword.FROM_SUBQUERY).append(unionSql).append(SqlFragment.CLOSE_PAREN).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_U);
        } else if (parsed.dedupRequested()) {
            finalSql = new StringBuilder(SqlKeyword.SELECT_DISTINCT + SqlKeyword.STAR).append(SqlKeyword.FROM_SUBQUERY).append(unionSql).append(SqlFragment.CLOSE_PAREN).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_U);
        } else {
            finalSql = new StringBuilder(unionSql);
        }

        // Prepend the CTE preamble (WITH _start AS (...)) if one was generated
        if (!ctePreamble.isEmpty()) {
            finalSql.insert(0, ctePreamble);
        }

        List<Object> allParams = new ArrayList<>();
        if (parsed.preHopLimit() != null) {
            allParams.addAll(sharedStartParams);
            allParams.addAll(sharedStartParams);
        } else {
            allParams.addAll(outParams);
            allParams.addAll(inParams);
        }

        SqlRenderHelper.appendLimit(finalSql, parsed.limit());
        return new TranslationResult(finalSql.toString(), allParams);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Appends the FROM clause for the main query body.
     *
     * <p>When {@code preHopLimit} is set and the dialect supports CTEs, the starting-vertex
     * subquery is emitted as a CTE ({@code WITH _start AS (...)}) instead of an inline
     * subquery.  This avoids the "correlated subquery trap" where the database re-evaluates
     * an EXISTS subquery for every row in the table before applying LIMIT, which causes
     * full-table scans and query timeouts on large datasets (e.g., 80 k+ AML transfers).
     *
     * <p>If CTEs are not supported the previous inline-subquery form is used as a fallback.
     *
     * @param ctePrefix    output — any CTE preamble ({@code WITH _start AS (…) }) is appended
     *                     here so callers can prepend it before the SELECT clause.
     * @param sql          main SQL buffer — receives the {@code FROM} clause reference.
     * @param params       parameter list — populated with bind values from the subquery.
     * @param parsed       parsed traversal state.
     * @param startVm      vertex mapping for the traversal's starting vertex type.
     */
    private void appendFromClause(StringBuilder ctePrefix, StringBuilder sql,
                                   List<Object> params, ParsedTraversal parsed,
                                   VertexMapping startVm) {
        if (parsed.preHopLimit() == null) {
            sql.append(SqlKeyword.FROM).append(startVm.table()).append(SqlFragment.SPACE).append(SqlFragment.V0);
            return;
        }

        // Build the inner SELECT for the starting vertex
        StringBuilder inner = new StringBuilder()
                .append(SqlKeyword.SELECT).append(SqlKeyword.STAR)
                .append(SqlKeyword.FROM).append(startVm.table());
        List<Object> subParams = new ArrayList<>();
        whereBuilder.appendWhereClauseForVertex(inner, subParams, parsed.filters(), startVm);
        if (parsed.whereClause() != null) {
            if (!whereBuilder.isStructuredWherePredicate(parsed.whereClause()))
                throw new IllegalArgumentException(
                        "Unsupported where() predicate for hop traversal: " + parsed.whereClause().kind());
            whereBuilder.appendStructuredWherePredicate(
                    inner, subParams, parsed.whereClause(), startVm, null,
                    !parsed.filters().isEmpty());
        }
        inner.append(SqlKeyword.LIMIT).append(parsed.preHopLimit());
        params.addAll(subParams);

        if (dialect.supportsCte()) {
            // CTE form: WITH _start AS (SELECT * FROM … WHERE … LIMIT n) SELECT … FROM _start v0
            ctePrefix.append(SqlKeyword.WITH)
                     .append(SqlFragment.START_ALIAS)
                     .append(SqlKeyword.AS_OPEN)
                     .append(inner)
                     .append(SqlKeyword.CLOSE_PAREN)
                     .append(SqlFragment.SPACE);
            sql.append(SqlKeyword.FROM).append(SqlFragment.START_ALIAS).append(SqlFragment.SPACE).append(SqlFragment.V0);
        } else {
            // Fallback: inline subquery (original behaviour)
            sql.append(SqlKeyword.FROM).append(SqlFragment.OPEN_PAREN).append(inner).append(SqlFragment.CLOSE_PAREN).append(SqlFragment.SPACE).append(SqlFragment.V0);
        }
    }

    /**
     * Convenience overload for callers that do not use the CTE prefix buffer
     * (used by the both-hop builder which manages its own from-clause).
     */
    private void appendFromClause(StringBuilder sql, List<Object> params, ParsedTraversal parsed,
                                   VertexMapping startVertexMapping) {
        StringBuilder ctePrefix = new StringBuilder();
        appendFromClause(ctePrefix, sql, params, parsed, startVertexMapping);
        if (!ctePrefix.isEmpty()) {
            // Insert the CTE prefix at position 0 of `sql` — caller's SELECT is already there
            sql.insert(0, ctePrefix);
        }
    }

    private boolean isEdgeHop(String direction) {
        return GremlinToken.OUT_E.equals(direction) || GremlinToken.IN_E.equals(direction) || GremlinToken.BOTH_E.equals(direction);
    }
}

