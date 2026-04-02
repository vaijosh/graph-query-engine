package com.graphqueryengine.query.factory;

import com.graphqueryengine.query.api.GraphQueryTranslator;
import com.graphqueryengine.query.parser.AntlrGremlinTraversalParser;
import com.graphqueryengine.query.parser.GremlinTraversalParser;
import com.graphqueryengine.query.parser.LegacyGremlinTraversalParser;
import com.graphqueryengine.query.translate.sql.GremlinSqlTranslator;
import com.graphqueryengine.query.translate.sql.SqlGraphQueryTranslator;

import java.util.Locale;

/**
 * Default translator selector. Today it supports SQL, while allowing future backends.
 */
public class DefaultGraphQueryTranslatorFactory implements GraphQueryTranslatorFactory {
    private final String backend;
    private final String parserMode;

    public DefaultGraphQueryTranslatorFactory() {
        this(
                System.getenv().getOrDefault("QUERY_TRANSLATOR_BACKEND", "sql"),
                System.getenv().getOrDefault("QUERY_PARSER", "antlr")
        );
    }

    public DefaultGraphQueryTranslatorFactory(String backend, String parserMode) {
        this.backend = backend == null ? "sql" : backend.trim().toLowerCase(Locale.ROOT);
        this.parserMode = parserMode == null ? "legacy" : parserMode.trim().toLowerCase(Locale.ROOT);
    }

    @Override
    public GraphQueryTranslator create() {
        if (backend.isBlank() || "sql".equals(backend) || "legacy-sql".equals(backend)) {
            return new SqlGraphQueryTranslator(resolveParser(), new GremlinSqlTranslator());
        }
        throw new IllegalStateException("Unsupported query translator backend: " + backend);
    }

    private GremlinTraversalParser resolveParser() {
        if (parserMode.isBlank() || "legacy".equals(parserMode) || "manual".equals(parserMode)) {
            return new LegacyGremlinTraversalParser();
        }
        if ("antlr".equals(parserMode)) {
            return new AntlrGremlinTraversalParser();
        }
        throw new IllegalStateException("Unsupported query parser mode: " + parserMode);
    }
}

