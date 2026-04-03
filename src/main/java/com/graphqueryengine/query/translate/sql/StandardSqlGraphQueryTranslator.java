package com.graphqueryengine.query.translate.sql;

import com.graphqueryengine.query.parser.GremlinTraversalParser;

/**
 * Dedicated translator entry point for standard SQL backends (e.g. H2).
 */
public class StandardSqlGraphQueryTranslator extends SqlGraphQueryTranslator {
    public StandardSqlGraphQueryTranslator(GremlinTraversalParser parser, GremlinSqlTranslator delegate) {
        super(parser, delegate, TranslationMode.STANDARD);
    }
}

