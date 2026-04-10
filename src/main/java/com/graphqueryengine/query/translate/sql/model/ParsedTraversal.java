package com.graphqueryengine.query.translate.sql.model;

import com.graphqueryengine.query.translate.sql.HasFilter;

import java.util.List;

/**
 * The fully-parsed representation of a single Gremlin traversal, ready for SQL generation.
 *
 * <p>Produced by {@link com.graphqueryengine.query.translate.sql.parse.GremlinStepParser} and
 * consumed by the SQL builder components.
 */
public record ParsedTraversal(
        String label,
        List<HasFilter> filters,
        String valueProperty,
        boolean valueMapRequested,
        List<String> valueMapKeys,
        Integer limit,
        Integer preHopLimit,
        List<HopStep> hops,
        boolean countRequested,
        boolean sumRequested,
        boolean meanRequested,
        List<ProjectionField> projections,
        String groupCountProperty,
        String orderByProperty,
        String orderDirection,
        List<AsAlias> asAliases,
        List<SelectField> selectFields,
        WhereClause whereClause,
        boolean dedupRequested,
        boolean pathSeen,
        List<String> pathByProperties,
        String whereByProperty,
        boolean simplePathRequested,
        GroupCountKeySpec groupCountKeySpec,
        boolean orderByCountDesc) {

    /**
     * Returns a copy of this traversal with the hops replaced by {@code newHops}.
     * Useful when splitting multi-label hops into single-label branches.
     */
    public ParsedTraversal withHops(List<HopStep> newHops) {
        return new ParsedTraversal(
                label, filters, valueProperty, valueMapRequested,
                valueMapKeys, limit, preHopLimit, newHops,
                countRequested, sumRequested, meanRequested,
                projections, groupCountProperty,
                orderByProperty, orderDirection,
                asAliases, selectFields, whereClause,
                dedupRequested, pathSeen, pathByProperties,
                whereByProperty, simplePathRequested,
                groupCountKeySpec, orderByCountDesc);
    }
}

