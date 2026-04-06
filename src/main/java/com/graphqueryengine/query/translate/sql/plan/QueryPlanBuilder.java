package com.graphqueryengine.query.translate.sql.plan;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.api.QueryPlan;
import com.graphqueryengine.query.translate.sql.dialect.SqlDialect;
import com.graphqueryengine.query.translate.sql.mapping.SqlMappingResolver;
import com.graphqueryengine.query.translate.sql.model.HopStep;
import com.graphqueryengine.query.translate.sql.model.ParsedTraversal;
import com.graphqueryengine.query.translate.sql.constant.GremlinToken;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds a {@link QueryPlan} from a {@link ParsedTraversal} without producing any SQL.
 *
 * <p>The plan captures every mapping-resolution decision made by the translator,
 * giving callers visibility into which tables and columns would be used without
 * actually executing the SQL.
 */
public class QueryPlanBuilder {

    private final SqlMappingResolver resolver;
    private final SqlDialect dialect;

    public QueryPlanBuilder(SqlMappingResolver resolver, SqlDialect dialect) {
        this.resolver = resolver;
        this.dialect  = dialect;
    }

    public QueryPlan build(boolean vertexQuery, ParsedTraversal t, MappingConfig mappingConfig) {
        String rootType = vertexQuery ? GremlinToken.TYPE_VERTEX : GremlinToken.TYPE_EDGE;

        // Root label + table
        String rootLabel = t.label();
        String rootTable;
        if (vertexQuery) {
            if (rootLabel == null && !mappingConfig.vertices().isEmpty()) {
                try { rootLabel = resolver.resolveSingleLabel(mappingConfig.vertices(), GremlinToken.TYPE_VERTEX); }
                catch (Exception ignored) {}
            }
            VertexMapping vm = rootLabel != null ? mappingConfig.vertices().get(rootLabel) : null;
            rootTable = vm != null ? vm.table() : null;
        } else {
            if (rootLabel == null && !mappingConfig.edges().isEmpty()) {
                try { rootLabel = resolver.resolveSingleLabel(mappingConfig.edges(), GremlinToken.TYPE_EDGE); }
                catch (Exception ignored) {}
            }
            EdgeMapping em = rootLabel != null ? mappingConfig.edges().get(rootLabel) : null;
            rootTable = em != null ? em.table() : null;
        }

        // ...existing code...

        // Filters
        List<QueryPlan.FilterPlan> filterPlans = t.filters().stream()
                .map(f -> new QueryPlan.FilterPlan(f.property(), f.operator(), f.value()))
                .toList();

        // Hops
        List<QueryPlan.HopPlan> hopPlans = new ArrayList<>();
        for (HopStep hop : t.hops()) {
            String edgeTable   = null;
            String targetTable = null;
            String targetLabel = null;
            if (!hop.labels().isEmpty()) {
                String firstLabel = hop.labels().get(0);
                EdgeMapping em = mappingConfig.edges().get(firstLabel);
                if (em != null) {
                    edgeTable = em.table();
                    boolean outDir = GremlinToken.OUT.equals(hop.direction()) || GremlinToken.OUT_E.equals(hop.direction());
                    VertexMapping tvm = resolver.resolveHopTargetVertexMapping(em, outDir, null);
                    if (tvm != null) {
                        targetTable = tvm.table();
                        for (Map.Entry<String, VertexMapping> e : mappingConfig.vertices().entrySet()) {
                            if (e.getValue() == tvm) { targetLabel = e.getKey(); break; }
                        }
                    }
                }
            }
            hopPlans.add(new QueryPlan.HopPlan(hop.direction(), hop.labels(), edgeTable, targetTable, targetLabel));
        }

        // Aggregation
        String aggregation         = null;
        String aggregationProperty = null;
        if (t.countRequested())                       { aggregation = GremlinToken.AGG_COUNT; }
        else if (t.sumRequested())                    { aggregation = GremlinToken.AGG_SUM;        aggregationProperty = t.valueProperty(); }
        else if (t.meanRequested())                   { aggregation = GremlinToken.AGG_MEAN;       aggregationProperty = t.valueProperty(); }
        else if (t.groupCountProperty() != null)      { aggregation = GremlinToken.AGG_GROUP_COUNT; aggregationProperty = t.groupCountProperty(); }
        else if (t.groupCountKeySpec() != null)       { aggregation = GremlinToken.AGG_GROUP_COUNT; }

        // Projections
        List<QueryPlan.ProjectionPlan> projPlans = t.projections().stream()
                .map(p -> new QueryPlan.ProjectionPlan(p.alias(), p.kind().name(), p.property()))
                .toList();

        // Where
        QueryPlan.WherePlan wherePlan = null;
        if (t.whereClause() != null) {
            var wc = t.whereClause();
            List<QueryPlan.FilterPlan> wFilters = wc.filters().stream()
                    .map(f -> new QueryPlan.FilterPlan(f.property(), f.operator(), f.value()))
                    .toList();
            wherePlan = new QueryPlan.WherePlan(wc.kind().name(), wc.left(), wc.right(),
                    wFilters.isEmpty() ? null : wFilters);
        }

        // As aliases
        List<QueryPlan.AliasPlan> aliasPlan = t.asAliases().stream()
                .map(a -> new QueryPlan.AliasPlan(a.label(), a.hopIndexAfter()))
                .toList();

        // Select fields
        List<QueryPlan.SelectPlan> selectPlan = t.selectFields().stream()
                .map(s -> new QueryPlan.SelectPlan(s.alias(), s.property()))
                .toList();

        return new QueryPlan(
                rootType,
                rootLabel,
                rootTable,
                dialect.getClass().getSimpleName(),
                filterPlans.isEmpty() ? null : filterPlans,
                (t.filters().isEmpty() || !GremlinToken.PROP_ID.equals(t.filters().get(0).property()))
                        ? null : t.filters().get(0).value(),
                hopPlans.isEmpty() ? null : hopPlans,
                t.simplePathRequested() ? true : null,
                aggregation,
                aggregationProperty,
                (t.valueProperty() != null && aggregation == null) ? t.valueProperty() : null,
                projPlans.isEmpty() ? null : projPlans,
                t.orderByProperty(),
                t.orderDirection(),
                t.orderByCountDesc() ? true : null,
                t.limit(),
                t.preHopLimit(),
                wherePlan,
                t.dedupRequested() ? true : null,
                (t.pathByProperties() == null || t.pathByProperties().isEmpty()) ? null : t.pathByProperties(),
                aliasPlan.isEmpty() ? null : aliasPlan,
                selectPlan.isEmpty() ? null : selectPlan);
    }
}

