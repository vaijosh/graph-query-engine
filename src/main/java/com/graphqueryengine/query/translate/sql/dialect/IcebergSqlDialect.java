package com.graphqueryengine.query.translate.sql.dialect;

/**
 * Iceberg/Trino dialect hook. Kept separate from Standard for backend-specific evolution.
 */
public class IcebergSqlDialect extends StandardSqlDialect {
	@Override
	public String stringAgg(String expressionSql, String delimiter) {
		String escapedDelimiter = delimiter.replace("'", "''");
		// Trino does not expose STRING_AGG in this runtime; use ARRAY_AGG + ARRAY_JOIN.
		return "ARRAY_JOIN(ARRAY_AGG(" + expressionSql + "), '" + escapedDelimiter + "')";
	}
}

