package com.graphqueryengine.query.translate.sql;

import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.query.api.GraphQueryTranslator;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.parser.GremlinTraversalParser;
import com.graphqueryengine.query.parser.model.GremlinParseResult;

/**
 * SQL translator pipeline:
 * parser mode (legacy/antlr) -> normalized parse model -> SQL compiler.
 */
public class SqlGraphQueryTranslator implements GraphQueryTranslator {
    private final GremlinTraversalParser parser;
    private final GremlinSqlTranslator delegate;

    public SqlGraphQueryTranslator(GremlinTraversalParser parser, GremlinSqlTranslator delegate) {
        this.parser = parser;
        this.delegate = delegate;
    }

    @Override
    public TranslationResult translate(String gremlin, MappingConfig mappingConfig) {
        GremlinParseResult parsed = parser.parse(gremlin);
        return delegate.translate(parsed, mappingConfig);
    }
}

