package com.graphqueryengine.query.translate.sql.render;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.translate.sql.HasFilter;
import com.graphqueryengine.query.translate.sql.mapping.SqlMappingResolver;
import com.graphqueryengine.query.translate.sql.model.AsAlias;
import com.graphqueryengine.query.translate.sql.model.HopStep;
import com.graphqueryengine.query.translate.sql.model.ParsedTraversal;
import com.graphqueryengine.query.translate.sql.model.ProjectionField;
import com.graphqueryengine.query.translate.sql.model.ProjectionKind;
import com.graphqueryengine.query.translate.sql.model.SelectField;
import com.graphqueryengine.query.translate.sql.model.WhereKind;
import com.graphqueryengine.query.translate.sql.where.WhereClauseBuilder;
import com.graphqueryengine.query.translate.sql.constant.GremlinToken;
import com.graphqueryengine.query.translate.sql.constant.SqlFragment;
import com.graphqueryengine.query.translate.sql.constant.SqlKeyword;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

import static com.graphqueryengine.query.translate.sql.render.VertexSqlBuilder.getTranslationResult;

/** Builds SQL for g.E() (edge-rooted) traversals. */
public class EdgeSqlBuilder {

    private final SqlMappingResolver resolver;
    private final WhereClauseBuilder whereBuilder;
    private final SqlRenderHelper helper;

    public EdgeSqlBuilder(SqlMappingResolver resolver, WhereClauseBuilder whereBuilder, SqlRenderHelper helper) {
        this.resolver = resolver;
        this.whereBuilder = whereBuilder;
        this.helper = helper;
    }

    public TranslationResult build(ParsedTraversal parsed, MappingConfig mappingConfig) {
        String label = parsed.label();
        if (label == null) label = resolver.resolveEdgeLabelFromFilters(parsed.filters());
        EdgeMapping edgeMapping = mappingConfig.edges().get(label);
        if (edgeMapping == null)
            throw new IllegalArgumentException("No edge mapping found for label: " + label);
        if (!parsed.selectFields().isEmpty())    return buildAliasSelectSql(parsed, edgeMapping);
        if (parsed.groupCountProperty() != null) return buildGroupCountSql(parsed, edgeMapping);
        if (!parsed.hops().isEmpty())            return buildEdgeToVertexSql(parsed, edgeMapping);
        if (!parsed.projections().isEmpty())     return buildProjectionSql(parsed, edgeMapping);
        return buildSimpleSelectSql(parsed, edgeMapping);
    }

    // ---- simple SELECT ----------------------------------------------------------

    private TranslationResult buildSimpleSelectSql(ParsedTraversal parsed, EdgeMapping em) {
        Integer limit = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();
        if (parsed.valueMapRequested()) {
            StringJoiner cols = new StringJoiner(SqlFragment.COMMA_SPACE);
            for (Map.Entry<String,String> e : em.properties().entrySet())
                cols.add(e.getValue() + SqlKeyword.AS + helper.quoteAlias(e.getKey()));
            List<Object> params = new ArrayList<>();
            StringBuilder sql = new StringBuilder(SqlKeyword.SELECT)
                    .append(parsed.dedupRequested() ? SqlKeyword.DISTINCT : SqlFragment.EMPTY).append(cols)
                    .append(SqlKeyword.FROM).append(em.table());
            whereBuilder.appendWhereClauseForEdge(sql, params, parsed.filters(), em);
            helper.appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
            SqlRenderHelper.appendLimit(sql, limit);
            return new TranslationResult(sql.toString(), params);
        }
        if (parsed.countRequested() && parsed.sumRequested())
            throw new IllegalArgumentException("count() cannot be combined with sum()");
        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder(SqlKeyword.SELECT).append(resolveSelectClause(parsed, em))
                .append(SqlKeyword.FROM).append(em.table());
        whereBuilder.appendWhereClauseForEdge(sql, params, parsed.filters(), em);
        helper.appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
        SqlRenderHelper.appendLimit(sql, limit);
        return new TranslationResult(sql.toString(), params);
    }

    private String resolveSelectClause(ParsedTraversal parsed, EdgeMapping em) {
        if (parsed.countRequested()) return SqlKeyword.COUNT_STAR;
        if (parsed.sumRequested()) {
            if (parsed.valueProperty() == null)
                throw new IllegalArgumentException("sum() requires values('property') before aggregation");
            return SqlKeyword.SUM_FN + resolver.mapEdgeProperty(em, parsed.valueProperty()) + SqlKeyword.SUM_ALIAS;
        }
        if (parsed.meanRequested()) {
            if (parsed.valueProperty() == null)
                throw new IllegalArgumentException("mean() requires values('property') before aggregation");
            return SqlKeyword.AVG_FN + resolver.mapEdgeProperty(em, parsed.valueProperty()) + SqlKeyword.MEAN_ALIAS;
        }
        if (parsed.valueProperty() != null)
            return resolver.mapEdgeProperty(em, parsed.valueProperty()) + SqlKeyword.AS + parsed.valueProperty();
        return SqlKeyword.STAR;
    }

    // ---- groupCount -------------------------------------------------------------

    public TranslationResult buildGroupCountSql(ParsedTraversal parsed, EdgeMapping em) {
        if (parsed.countRequested())         throw new IllegalArgumentException("count() cannot be combined with groupCount()");
        if (parsed.valueProperty() != null)  throw new IllegalArgumentException("values() cannot be combined with groupCount()");
        if (!parsed.projections().isEmpty()) throw new IllegalArgumentException("project(...) cannot be combined with groupCount()");
        String gc  = parsed.groupCountProperty();
        String col = resolver.mapEdgeProperty(em, gc);
        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder(SqlKeyword.SELECT).append(col).append(SqlKeyword.AS)
                .append(helper.quoteAlias(gc))
                .append(SqlFragment.COMMA_SPACE).append(SqlKeyword.COUNT_STAR).append(SqlKeyword.FROM).append(em.table());
        whereBuilder.appendWhereClauseForEdge(sql, params, parsed.filters(), em);
        return getTranslationResult(parsed, col, params, sql, helper);
    }

    // ---- project(...) -----------------------------------------------------------

    private TranslationResult buildProjectionSql(ParsedTraversal parsed, EdgeMapping em) {
        if (parsed.countRequested())        throw new IllegalArgumentException("count() cannot be combined with project(...)");
        if (parsed.valueProperty() != null) throw new IllegalArgumentException("values() cannot be combined with project(...)");
        List<Object> params = new ArrayList<>();
        StringJoiner sel = new StringJoiner(SqlFragment.COMMA_SPACE);
        boolean needsOut = false, needsIn = false;
        VertexMapping outVm = resolver.resolveHopTargetVertexMapping(em, false, null);
        VertexMapping inVm  = resolver.resolveHopTargetVertexMapping(em, true,  null);
        if (outVm == null || inVm == null) {
            List<String> outProps = new ArrayList<>(), inProps = new ArrayList<>();
            for (ProjectionField pf : parsed.projections()) {
                if (pf.kind() == ProjectionKind.OUT_VERTEX_PROPERTY) outProps.add(pf.property());
                if (pf.kind() == ProjectionKind.IN_VERTEX_PROPERTY)  inProps.add(pf.property());
            }
            VertexMapping fb = resolver.resolveVertexMappingForEdgeProjection();
            if (outVm == null) { outVm = resolver.resolveVertexMappingByProperties(outProps); if (outVm == null) outVm = fb; }
            if (inVm  == null) { inVm  = resolver.resolveVertexMappingByProperties(inProps);  if (inVm  == null) inVm  = fb; }
            if (outVm == null || inVm == null)
                throw new IllegalArgumentException("project(...).by(outV()/inV()) requires resolvable vertex mappings");
        }
        for (ProjectionField p : parsed.projections()) {
            String a = helper.quoteAlias(p.alias());
            if      (p.kind() == ProjectionKind.IDENTITY)            sel.add(SqlFragment.PREFIX_E_DOT + em.idColumn() + SqlKeyword.AS + a);
            else if (p.kind() == ProjectionKind.EDGE_PROPERTY)       sel.add(SqlFragment.PREFIX_E_DOT + resolver.mapEdgeProperty(em, p.property()) + SqlKeyword.AS + a);
            else if (p.kind() == ProjectionKind.OUT_VERTEX_PROPERTY) { sel.add(SqlFragment.PREFIX_OV + resolver.mapVertexProperty(outVm, p.property()) + SqlKeyword.AS + a); needsOut = true; }
            else if (p.kind() == ProjectionKind.IN_VERTEX_PROPERTY)  { sel.add(SqlFragment.PREFIX_IV + resolver.mapVertexProperty(inVm,  p.property()) + SqlKeyword.AS + a); needsIn  = true; }
        }
        StringBuilder sql = new StringBuilder(SqlKeyword.SELECT).append(sel).append(SqlKeyword.FROM).append(em.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_E);
        if (needsOut) sql.append(SqlKeyword.JOIN).append(outVm.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_OV).append(SqlKeyword.ON).append(SqlFragment.PREFIX_OV).append(outVm.idColumn()).append(SqlFragment.SPACE_EQ_SPACE).append(SqlFragment.PREFIX_E_DOT).append(em.outColumn());
        if (needsIn)  sql.append(SqlKeyword.JOIN).append(inVm.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_IV).append(SqlKeyword.ON).append(SqlFragment.PREFIX_IV).append(inVm.idColumn()).append(SqlFragment.SPACE_EQ_SPACE).append(SqlFragment.PREFIX_E_DOT).append(em.inColumn());
        whereBuilder.appendWhereClauseForEdgeAlias(sql, params, parsed.filters(), em, SqlFragment.ALIAS_E);
        SqlRenderHelper.appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());
        return new TranslationResult(sql.toString(), params);
    }

    // ---- select(...).by(...) pattern --------------------------------------------

    private TranslationResult buildAliasSelectSql(ParsedTraversal parsed, EdgeMapping rootEdge) {
        if (parsed.countRequested())        throw new IllegalArgumentException("count() cannot be combined with select(...)");
        if (parsed.valueProperty() != null) throw new IllegalArgumentException("values() cannot be combined with select(...)");
        if (parsed.whereClause() != null && parsed.whereClause().kind() != WhereKind.NEQ_ALIAS)
            throw new IllegalArgumentException("Only where('a',neq('b')) is supported with select(...)");
        List<HopStep> hops = parsed.hops();
        if (hops.size() == 3
                && GremlinToken.OUT_V.equals(hops.get(0).direction())
                && (GremlinToken.OUT_E.equals(hops.get(1).direction()) || GremlinToken.IN_E.equals(hops.get(1).direction()))
                && (GremlinToken.IN_V.equals(hops.get(2).direction())  || GremlinToken.OUT_V.equals(hops.get(2).direction()))) {
            VertexMapping vm = Optional.ofNullable(resolver.resolveVertexMappingForEdgeProjection())
                    .orElseThrow(() -> new IllegalArgumentException("select(...).by('prop') from g.E() requires exactly one vertex mapping"));
            Map<String,Integer> ahIdx = new HashMap<>();
            for (AsAlias aa : parsed.asAliases()) ahIdx.put(aa.label(), aa.hopIndexAfter());
            EdgeMapping e2 = resolver.resolveEdgeMapping(hops.get(1).singleLabel());
            List<Object> params = new ArrayList<>();
            StringJoiner sel = new StringJoiner(SqlFragment.COMMA_SPACE);
            for (SelectField f : parsed.selectFields()) {
                Integer hi = ahIdx.get(f.alias());
                if (hi == null) throw new IllegalArgumentException("select alias not found: " + f.alias());
                if (f.property() == null || f.property().isBlank())
                    throw new IllegalArgumentException("select(...).by(...) requires a property for alias: " + f.alias());
                sel.add(SqlFragment.V_PREFIX + hi + SqlFragment.DOT + resolver.mapVertexProperty(vm, f.property()) + SqlKeyword.AS + helper.quoteAlias(f.alias()));
            }
            StringBuilder sql = new StringBuilder(SqlKeyword.SELECT_DISTINCT).append(sel)
                    .append(SqlKeyword.FROM).append(rootEdge.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_E0)
                    .append(SqlKeyword.JOIN).append(vm.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_V1).append(SqlKeyword.ON).append(SqlFragment.PREFIX_V1).append(vm.idColumn()).append(SqlFragment.SPACE_EQ_SPACE).append(SqlFragment.PREFIX_E0).append(rootEdge.outColumn())
                    .append(SqlKeyword.JOIN).append(e2.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_E2);
            sql.append(GremlinToken.OUT_E.equals(hops.get(1).direction())
                    ? SqlKeyword.ON + SqlFragment.PREFIX_E2 + e2.outColumn() + SqlFragment.SPACE_EQ_SPACE + SqlFragment.PREFIX_V1 + vm.idColumn()
                    : SqlKeyword.ON + SqlFragment.PREFIX_E2 + e2.inColumn()  + SqlFragment.SPACE_EQ_SPACE + SqlFragment.PREFIX_V1 + vm.idColumn());
            sql.append(SqlKeyword.JOIN).append(vm.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_V3);
            sql.append(GremlinToken.IN_V.equals(hops.get(2).direction())
                    ? SqlKeyword.ON + SqlFragment.PREFIX_V3 + vm.idColumn() + SqlFragment.SPACE_EQ_SPACE + SqlFragment.PREFIX_E2 + e2.inColumn()
                    : SqlKeyword.ON + SqlFragment.PREFIX_V3 + vm.idColumn() + SqlFragment.SPACE_EQ_SPACE + SqlFragment.PREFIX_E2 + e2.outColumn());
            StringJoiner wj = new StringJoiner(SqlKeyword.AND);
            List<HasFilter> ef = parsed.filters();
            if (!ef.isEmpty()) {
                wj.add(SqlFragment.PREFIX_E0 + resolver.mapEdgeFilterProperty(rootEdge, ef.get(0).property()) + SqlFragment.SPACE_EQ_SPACE + SqlKeyword.PLACEHOLDER);
                params.add(ef.get(0).typedValue());
                for (int i = 1; i < ef.size(); i++) {
                    wj.add(SqlFragment.PREFIX_E2 + resolver.mapEdgeFilterProperty(e2, ef.get(i).property()) + SqlFragment.SPACE_EQ_SPACE + SqlKeyword.PLACEHOLDER);
                    params.add(ef.get(i).typedValue());
                }
            }
            if (parsed.whereClause() != null) {
                wj.add(resolver.resolveVertexAliasForWhere(parsed.whereClause().left(), ahIdx) + SqlFragment.DOT + vm.idColumn()
                     + SqlKeyword.NEQ + resolver.resolveVertexAliasForWhere(parsed.whereClause().right(), ahIdx) + SqlFragment.DOT + vm.idColumn());
            }
            String ws = wj.toString();
            if (!ws.isBlank()) sql.append(SqlKeyword.WHERE).append(ws);
            helper.appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
            SqlRenderHelper.appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());
            return new TranslationResult(sql.toString(), params);
        }
        throw new IllegalArgumentException("select(...).by(...) from g.E() currently supports outV().outE()/inE().inV()/outV() pattern");
    }

    // ---- outV / inV / bothV hop -------------------------------------------------

    private TranslationResult buildEdgeToVertexSql(ParsedTraversal parsed, EdgeMapping em) {
        if (parsed.hops().size() != 1)
            throw new IllegalArgumentException("Only a single outV(), inV(), or bothV() hop is supported after g.E()");
        HopStep hop = parsed.hops().get(0);
        if (!GremlinToken.OUT_V.equals(hop.direction()) && !GremlinToken.IN_V.equals(hop.direction()) && !GremlinToken.BOTH_V.equals(hop.direction()))
            throw new IllegalArgumentException("Only outV(), inV(), or bothV() hops are supported after g.E()");
        VertexMapping outVm = resolver.resolveTargetVertexMapping(em, false);
        VertexMapping inVm  = resolver.resolveTargetVertexMapping(em, true);
        VertexMapping fb    = resolver.resolveVertexMappingForEdgeProjection();
        if (outVm == null) outVm = fb;
        if (inVm  == null) inVm  = fb;
        if (outVm == null || inVm == null)
            throw new IllegalArgumentException("outV()/inV()/bothV() traversal requires resolvable endpoint vertex mappings");
        Integer limit = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();
        if (GremlinToken.BOTH_V.equals(hop.direction())) {
            List<Object> op = new ArrayList<>(), ip = new ArrayList<>();
            StringBuilder oSql = new StringBuilder(SqlKeyword.SELECT).append(helper.buildEdgeEndpointSelectClause(parsed, outVm))
                    .append(SqlKeyword.FROM).append(outVm.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_V).append(SqlKeyword.JOIN).append(em.table())
                    .append(SqlFragment.SPACE).append(SqlFragment.ALIAS_E).append(SqlKeyword.ON).append(SqlFragment.PREFIX_V).append(outVm.idColumn()).append(SqlFragment.SPACE_EQ_SPACE).append(SqlFragment.PREFIX_E_DOT).append(em.outColumn());
            whereBuilder.appendWhereClauseForEdgeAlias(oSql, op, parsed.filters(), em, SqlFragment.ALIAS_E);
            StringBuilder iSql = new StringBuilder(SqlKeyword.SELECT).append(helper.buildEdgeEndpointSelectClause(parsed, inVm))
                    .append(SqlKeyword.FROM).append(inVm.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_V).append(SqlKeyword.JOIN).append(em.table())
                    .append(SqlFragment.SPACE).append(SqlFragment.ALIAS_E).append(SqlKeyword.ON).append(SqlFragment.PREFIX_V).append(inVm.idColumn()).append(SqlFragment.SPACE_EQ_SPACE).append(SqlFragment.PREFIX_E_DOT).append(em.inColumn());
            whereBuilder.appendWhereClauseForEdgeAlias(iSql, ip, parsed.filters(), em, SqlFragment.ALIAS_E);
            StringBuilder union = new StringBuilder(SqlKeyword.SELECT_DISTINCT + SqlKeyword.STAR).append(SqlKeyword.FROM_SUBQUERY)
                    .append(oSql).append(SqlKeyword.UNION_ALL).append(iSql).append(SqlFragment.CLOSE_PAREN).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_UV);
            helper.appendOrderBy(union, parsed.orderByProperty(), parsed.orderDirection());
            SqlRenderHelper.appendLimit(union, limit);
            List<Object> all = new ArrayList<>(); all.addAll(op); all.addAll(ip);
            return new TranslationResult(union.toString(), all);
        }
        boolean isOut = GremlinToken.OUT_V.equals(hop.direction());
        VertexMapping epm = isOut ? outVm : inVm;
        String jc = isOut ? em.outColumn() : em.inColumn();
        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder(SqlKeyword.SELECT_DISTINCT).append(helper.buildEdgeEndpointSelectClause(parsed, epm))
                .append(SqlKeyword.FROM).append(epm.table()).append(SqlFragment.SPACE).append(SqlFragment.ALIAS_V).append(SqlKeyword.JOIN).append(em.table())
                .append(SqlFragment.SPACE).append(SqlFragment.ALIAS_E).append(SqlKeyword.ON).append(SqlFragment.PREFIX_V).append(epm.idColumn()).append(SqlFragment.SPACE_EQ_SPACE).append(SqlFragment.PREFIX_E_DOT).append(jc);
        whereBuilder.appendWhereClauseForEdgeAlias(sql, params, parsed.filters(), em, SqlFragment.ALIAS_E);
        helper.appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
        SqlRenderHelper.appendLimit(sql, limit);
        return new TranslationResult(sql.toString(), params);
    }
}
