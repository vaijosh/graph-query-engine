package com.graphqueryengine.query.translate.sql.dialect;

public class StandardSqlDialect implements SqlDialect {
    @Override
    public String quoteIdentifier(String identifier) {
        String escaped = identifier.replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }

    @Override
    public String stringAgg(String expressionSql, String delimiter) {
        return "STRING_AGG(" + expressionSql + ", '" + delimiter.replace("'", "''") + "')";
    }
}

