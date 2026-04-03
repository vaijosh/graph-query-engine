package com.graphqueryengine.query.translate.sql.dialect;

/**
 * Small SQL dialect surface used by GremlinSqlTranslator.
 * This keeps traversal planning shared while allowing backend-specific rendering.
 */
public interface SqlDialect {
    String quoteIdentifier(String identifier);

    String stringAgg(String expressionSql, String delimiter);
}

