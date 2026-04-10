package com.graphqueryengine.query.factory;

import com.graphqueryengine.query.api.GraphQueryTranslator;
import com.graphqueryengine.query.parser.AntlrGremlinTraversalParser;
import com.graphqueryengine.query.translate.sql.GremlinSqlTranslator;
import com.graphqueryengine.query.translate.sql.IcebergSqlGraphQueryTranslator;
import com.graphqueryengine.query.translate.sql.StandardSqlGraphQueryTranslator;
import com.graphqueryengine.query.translate.sql.dialect.IcebergSqlDialect;
import com.graphqueryengine.query.translate.sql.dialect.StandardSqlDialect;

import java.util.Locale;

/**
 * Default translator selector. Today it supports SQL, while allowing future backends.
 * The parser is always ANTLR ({@link AntlrGremlinTraversalParser}).
 */
public class DefaultGraphQueryTranslatorFactory implements GraphQueryTranslatorFactory {
    private final String backend;

    public DefaultGraphQueryTranslatorFactory() {
        this(System.getenv().getOrDefault("QUERY_TRANSLATOR_BACKEND", "sql"));
    }

    public DefaultGraphQueryTranslatorFactory(String backend) {
        this.backend = backend == null ? "sql" : backend.trim().toLowerCase(Locale.ROOT);
    }


    @Override
    public GraphQueryTranslator create() {
        AntlrGremlinTraversalParser parser = new AntlrGremlinTraversalParser();
        if (backend.isBlank() || "sql".equals(backend) || "legacy-sql".equals(backend)) {
            return new StandardSqlGraphQueryTranslator(parser, new GremlinSqlTranslator(new StandardSqlDialect()));
        }
        if ("iceberg".equals(backend) || "iceberg-sql".equals(backend)) {
            return new IcebergSqlGraphQueryTranslator(parser, new GremlinSqlTranslator(new IcebergSqlDialect()));
        }
        throw new IllegalStateException("Unsupported query translator backend: " + backend);
    }
}
