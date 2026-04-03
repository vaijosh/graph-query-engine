package com.graphqueryengine.query.translate.sql;

import com.graphqueryengine.query.parser.GremlinTraversalParser;

/**
 * Dedicated translator entry point for Iceberg SQL backends (e.g. Trino + Iceberg catalog).
 */
public class IcebergSqlGraphQueryTranslator extends SqlGraphQueryTranslator {
    public IcebergSqlGraphQueryTranslator(GremlinTraversalParser parser, GremlinSqlTranslator delegate) {
        super(parser, delegate, TranslationMode.ICEBERG);
    }
}

