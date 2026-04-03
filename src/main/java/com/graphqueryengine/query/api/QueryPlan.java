package com.graphqueryengine.query.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;

/**
 * Structured representation of the traversal decisions made by
 * {@code GremlinSqlTranslator} before SQL is rendered.
 *
 * <p>Every field maps directly to an internal concept in the translator.
 * Fields that are not applicable to a given query are omitted from JSON
 * output ({@link JsonInclude.Include#NON_NULL}).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record QueryPlan(

        // ── Root ─────────────────────────────────────────────────────────────
        /** "vertex" or "edge" — which side of the graph this query starts from. */
        String rootType,

        /** Resolved vertex/edge label (e.g. "Account", "TRANSFER"). */
        String rootLabel,

        /** Resolved SQL table for the root label. */
        String rootTable,

        /** The SQL dialect in use (e.g. "StandardSqlDialect", "IcebergSqlDialect"). */
        String dialect,

        // ── Filters ──────────────────────────────────────────────────────────
        /** has(k,v) filters applied to the root vertex/edge. */
        List<FilterPlan> filters,

        /** rootIdFilter value, e.g. "1" from g.V(1). */
        String rootIdFilter,

        // ── Traversal ────────────────────────────────────────────────────────
        /** Sequence of resolved hop steps. */
        List<HopPlan> hops,

        /** Whether simplePath() cycle-exclusion is active. */
        Boolean simplePath,

        // ── Aggregation ──────────────────────────────────────────────────────
        /** "count", "sum", "mean", "groupCount", or null for row-level queries. */
        String aggregation,

        /** Property used for sum/mean/groupCount. */
        String aggregationProperty,

        /** Property used for values() projection. */
        String valuesProperty,

        // ── Projections ──────────────────────────────────────────────────────
        /** Named project(...).by(...) fields. */
        List<ProjectionPlan> projections,

        // ── Ordering / Paging ─────────────────────────────────────────────────
        /** Resolved ORDER BY column or alias. */
        String orderBy,

        /** "ASC" or "DESC". */
        String orderDirection,

        /** Whether ordering is by count-desc (groupCount result). */
        Boolean orderByCountDesc,

        /** LIMIT applied after hops/projection. */
        Integer limit,

        /** Pre-hop LIMIT applied inside the start-vertex subquery. */
        Integer preHopLimit,

        // ── Where / Path ─────────────────────────────────────────────────────
        /** Resolved where() clause, if any. */
        WherePlan where,

        /** Whether dedup() is requested. */
        Boolean dedup,

        /** path().by(...) property names in order. */
        List<String> pathByProperties,

        // ── Alias / Select ────────────────────────────────────────────────────
        /** as(...) aliases registered during the traversal. */
        List<AliasPlan> asAliases,

        /** select(...).by(...) fields. */
        List<SelectPlan> selectFields

) {

    // ── Nested plan records ───────────────────────────────────────────────────

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record FilterPlan(String property, String operator, String value) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record HopPlan(
            String direction,
            List<String> labels,
            String resolvedEdgeTable,
            String resolvedTargetTable,
            String resolvedTargetLabel
    ) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ProjectionPlan(String alias, String kind, String detail) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record WherePlan(String kind, String left, String right, List<FilterPlan> filters) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record AliasPlan(String label, int hopIndexAfter) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SelectPlan(String alias, String property) {}
}

