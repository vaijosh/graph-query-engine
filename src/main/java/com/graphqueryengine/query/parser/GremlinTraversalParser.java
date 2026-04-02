package com.graphqueryengine.query.parser;

import com.graphqueryengine.query.parser.model.GremlinParseResult;

/**
 * Parses incoming Gremlin into a normalized traversal model used by translation.
 */
public interface GremlinTraversalParser {
    /**
     * Parses and validates Gremlin text and returns a normalized traversal model.
     */
    GremlinParseResult parse(String gremlin);

    /**
     * Convenience validation hook retained for compatibility with existing call sites/tests.
     */
    default void validate(String gremlin) {
        parse(gremlin);
    }
}

