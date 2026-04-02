package com.graphqueryengine.query.parser.model;

import java.util.List;

/**
 * Parsed Gremlin traversal model shared between parser implementations and SQL translation.
 */
public record GremlinParseResult(
        boolean vertexQuery,
        String rootIdFilter,
        List<GremlinStep> steps
) {
}

