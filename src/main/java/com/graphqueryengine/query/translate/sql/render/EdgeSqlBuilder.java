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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

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
            StringJoiner cols = new StringJoiner(", ");
            for (Map.Entry<String,String> e : em.properties().entrySet())
                cols.add(e.getValue() + " AS " + helper.quoteAlias(e.getKey()));
            List<Object> params = new ArrayList<>();
            StringBuilder sql = new StringBuilder("SELECT ")
                    .append(parsed.dedupRequested() ? "DISTINCT " : "").append(cols).append(" FROM ").append(em.table());
            whereBuilder.appendWhereClauseForEdge(sql, params, parsed.filters(), em);
            helper.appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
            SqlRenderHelper.appendLimit(sql, limit);
            return new TranslationResult(sql.toString(), params);
        }
        if (parsed.countRequested() && parsed.sumRequested())
            throw new IllegalArgumentException("count() cannot be combined with sum()");
        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT ").append(resolveSelectClause(parsed, em))
                .append(" FROM ").append(em.table());
        whereBuilder.appendWhereClauseForEdge(sql, params, parsed.filters(), em);
        helper.appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
        SqlRenderHelper.appendLimit(sql, limit);
        return new TranslationResult(sql.toString(), params);
    }

    private String resolveSelectClause(ParsedTraversal parsed, EdgeMapping em) {
        if (parsed.countRequested()) return "COUNT(*) AS count";
        if (parsed.sumRequested()) {
            if (parsed.valueProperty() == null)
                throw new IllegalArgumentException("sum() requires values('property') before aggregation");
            return "SUM(" + resolver.mapEdgeProperty(em, parsed.valueProperty()) + ") AS sum";
        }
        if (parsed.meanRequested()) {
            if (parsed.valueProperty() == null)
                throw new IllegalArgumentException("mean() requires values('property') before aggregation");
            return "AVG(" + resolver.mapEdgeProperty(em, parsed.valueProperty()) + ") AS mean";
        }
        if (parsed.valueProperty() != null)
            return resolver.mapEdgeProperty(em, parsed.valueProperty()) + " AS " + parsed.valueProperty();
        return "*";
    }

    // ---- groupCount -------------------------------------------------------------

    public TranslationResult buildGroupCountSql(ParsedTraversal parsed, EdgeMapping em) {
        if (parsed.countRequested())         throw new IllegalArgumentException("count() cannot be combined with groupCount()");
        if (parsed.valueProperty() != null)  throw new IllegalArgumentException("values() cannot be combined with groupCount()");
        if (!parsed.projections().isEmpty()) throw new IllegalArgumentException("project(...) cannot be combined with groupCount()");
        String gc  = parsed.groupCountProperty();
        String col = resolver.mapEdgeProperty(em, gc);
        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT ").append(col).append(" AS ").append(helper.quoteAlias(gc))
                .append(", COUNT(*) AS count FROM ").append(em.table());
        whereBuilder.appendWhereClauseForEdge(sql, params, parsed.filters(), em);
        sql.append(" GROUP BY ").append(col);
        if (parsed.orderByCountDesc())
            sql.append(" ORDER BY count ").append(parsed.orderDirection() != null ? parsed.orderDirection() : "DESC");
        else
            helper.appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
        SqlRenderHelper.appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());
        return new TranslationResult(sql.toString(), params);
    }

    // ---- project(...) -----------------------------------------------------------

    private TranslationResult buildProjectionSql(ParsedTraversal parsed, EdgeMapping em) {
        if (parsed.countRequested())        throw new IllegalArgumentException("count() cannot be combined with project(...)");
        if (parsed.valueProperty() != null) throw new IllegalArgumentException("values() cannot be combined with project(...)");
        List<Object> params = new ArrayList<>();
        StringJoiner sel = new StringJoiner(", ");
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
            if      (p.kind() == ProjectionKind.IDENTITY)            sel.add("e." + em.idColumn() + " AS " + a);
            else if (p.kind() == ProjectionKind.EDGE_PROPERTY)       sel.add("e."  + resolver.mapEdgeProperty(em, p.property()) + " AS " + a);
            else if (p.kind() == ProjectionKind.OUT_VERTEX_PROPERTY) { sel.add("ov." + resolver.mapVertexProperty(outVm, p.property()) + " AS " + a); needsOut = true; }
            else if (p.kind() == ProjectionKind.IN_VERTEX_PROPERTY)  { sel.add("iv." + resolver.mapVertexProperty(inVm,  p.property()) + " AS " + a); needsIn  = true; }
        }
        StringBuilder sql = new StringBuilder("SELECT ").append(sel).append(" FROM ").append(em.table()).append(" e");
        if (needsOut) sql.append(" JOIN ").append(outVm.table()).append(" ov ON ov.").append(outVm.idColumn()).append(" = e.").append(em.outColumn());
        if (needsIn)  sql.append(" JOIN ").append(inVm.table()).append(" iv ON iv.").append(inVm.idColumn()).append(" = e.").append(em.inColumn());
        whereBuilder.appendWhereClauseForEdgeAlias(sql, params, parsed.filters(), em, "e");
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
                && "outV".equals(hops.get(0).direction())
                && ("outE".equals(hops.get(1).direction()) || "inE".equals(hops.get(1).direction()))
                && ("inV".equals(hops.get(2).direction())  || "outV".equals(hops.get(2).direction()))) {
            VertexMapping vm = Optional.ofNullable(resolver.resolveVertexMappingForEdgeProjection())
                    .orElseThrow(() -> new IllegalArgumentException("select(...).by('prop') from g.E() requires exactly one vertex mapping"));
            Map<String,Integer> ahIdx = new HashMap<>();
            for (AsAlias aa : parsed.asAliases()) ahIdx.put(aa.label(), aa.hopIndexAfter());
            EdgeMapping e2 = resolver.resolveEdgeMapping(hops.get(1).singleLabel());
            List<Object> params = new ArrayList<>();
            StringJoiner sel = new StringJoiner(", ");
            for (SelectField f : parsed.selectFields()) {
                Integer hi = ahIdx.get(f.alias());
                if (hi == null) throw new IllegalArgumentException("select alias not found: " + f.alias());
                if (f.property() == null || f.property().isBlank())
                    throw new IllegalArgumentException("select(...).by(...) requires a property for alias: " + f.alias());
                sel.add("v" + hi + "." + resolver.mapVertexProperty(vm, f.property()) + " AS " + helper.quoteAlias(f.alias()));
            }
            StringBuilder sql = new StringBuilder("SELECT DISTINCT ").append(sel)
                    .append(" FROM ").append(rootEdge.table()).append(" e0")
                    .append(" JOIN ").append(vm.table()).append(" v1 ON v1.").append(vm.idColumn()).append(" = e0.").append(rootEdge.outColumn())
                    .append(" JOIN ").append(e2.table()).append(" e2");
            sql.append("outE".equals(hops.get(1).direction())
                    ? " ON e2." + e2.outColumn() + " = v1." + vm.idColumn()
                    : " ON e2." + e2.inColumn()  + " = v1." + vm.idColumn());
            sql.append(" JOIN ").append(vm.table()).append(" v3");
            sql.append("inV".equals(hops.get(2).direction())
                    ? " ON v3." + vm.idColumn() + " = e2." + e2.inColumn()
                    : " ON v3." + vm.idColumn() + " = e2." + e2.outColumn());
            StringJoiner wj = new StringJoiner(" AND ");
            List<HasFilter> ef = parsed.filters();
            if (!ef.isEmpty()) {
                wj.add("e0." + resolver.mapEdgeFilterProperty(rootEdge, ef.get(0).property()) + " = ?");
                params.add(ef.get(0).value());
                for (int i = 1; i < ef.size(); i++) {
                    wj.add("e2." + resolver.mapEdgeFilterProperty(e2, ef.get(i).property()) + " = ?");
                    params.add(ef.get(i).value());
                }
            }
            if (parsed.whereClause() != null) {
                wj.add(resolver.resolveVertexAliasForWhere(parsed.whereClause().left(), ahIdx) + "." + vm.idColumn()
                     + " <> " + resolver.resolveVertexAliasForWhere(parsed.whereClause().right(), ahIdx) + "." + vm.idColumn());
            }
            String ws = wj.toString();
            if (!ws.isBlank()) sql.append(" WHERE ").append(ws);
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
        if (!"outV".equals(hop.direction()) && !"inV".equals(hop.direction()) && !"bothV".equals(hop.direction()))
            throw new IllegalArgumentException("Only outV(), inV(), or bothV() hops are supported after g.E()");
        VertexMapping outVm = resolver.resolveTargetVertexMapping(em, false);
        VertexMapping inVm  = resolver.resolveTargetVertexMapping(em, true);
        VertexMapping fb    = resolver.resolveVertexMappingForEdgeProjection();
        if (outVm == null) outVm = fb;
        if (inVm  == null) inVm  = fb;
        if (outVm == null || inVm == null)
            throw new IllegalArgumentException("outV()/inV()/bothV() traversal requires resolvable endpoint vertex mappings");
        Integer limit = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();
        if ("bothV".equals(hop.direction())) {
            List<Object> op = new ArrayList<>(), ip = new ArrayList<>();
            StringBuilder oSql = new StringBuilder("SELECT ").append(helper.buildEdgeEndpointSelectClause(parsed, outVm))
                    .append(" FROM ").append(outVm.table()).append(" v JOIN ").append(em.table())
                    .append(" e ON v.").append(outVm.idColumn()).append(" = e.").append(em.outColumn());
            whereBuilder.appendWhereClauseForEdgeAlias(oSql, op, parsed.filters(), em, "e");
            StringBuilder iSql = new StringBuilder("SELECT ").append(helper.buildEdgeEndpointSelectClause(parsed, inVm))
                    .append(" FROM ").append(inVm.table()).append(" v JOIN ").append(em.table())
                    .append(" e ON v.").append(inVm.idColumn()).append(" = e.").append(em.inColumn());
            whereBuilder.appendWhereClauseForEdgeAlias(iSql, ip, parsed.filters(), em, "e");
            StringBuilder union = new StringBuilder("SELECT DISTINCT * FROM (").append(oSql).append(" UNION ALL ").append(iSql).append(") _uv");
            helper.appendOrderBy(union, parsed.orderByProperty(), parsed.orderDirection());
            SqlRenderHelper.appendLimit(union, limit);
            List<Object> all = new ArrayList<>(); all.addAll(op); all.addAll(ip);
            return new TranslationResult(union.toString(), all);
        }
        boolean isOut = "outV".equals(hop.direction());
        VertexMapping epm = isOut ? outVm : inVm;
        String jc = isOut ? em.outColumn() : em.inColumn();
        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT DISTINCT ").append(helper.buildEdgeEndpointSelectClause(parsed, epm))
                .append(" FROM ").append(epm.table()).append(" v JOIN ").append(em.table())
                .append(" e ON v.").append(epm.idColumn()).append(" = e.").append(jc);
        whereBuilder.appendWhereClauseForEdgeAlias(sql, params, parsed.filters(), em, "e");
        helper.appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
        SqlRenderHelper.appendLimit(sql, limit);
        return new TranslationResult(sql.toString(), params);
    }
}
