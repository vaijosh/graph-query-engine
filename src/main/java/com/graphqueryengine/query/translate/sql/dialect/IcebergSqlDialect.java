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

	/**
	 * Trino/Iceberg enforces strict type matching for JDBC parameters.
	 *
	 * <p>The default implementation (identity) is intentionally kept here — we do NOT
	 * blindly coerce all numeric-looking strings to {@link Long} because many Iceberg
	 * columns are VARCHAR even when they hold numeric-looking values (e.g. {@code is_laundering},
	 * {@code transaction_id}).  Coercing those would flip the error from
	 * {@code bigint = varchar} to {@code varchar = integer}.
	 *
	 * <p>Column-type-specific coercions are applied directly in
	 * {@link com.graphqueryengine.query.translate.sql.where.WhereClauseBuilder} at the
	 * exact sites where the column type is known:
	 * <ul>
	 *   <li>ID column comparisons ({@code hasId}, {@code WHERE id = ?}) → {@link Long}</li>
	 *   <li>COUNT subquery comparisons ({@code count().is(n)}) → {@link Long}</li>
	 *   <li>Range operators on numeric columns → already handled by
	 *       {@link com.graphqueryengine.query.translate.sql.HasFilter#typedValue()}</li>
	 *   <li>All other equality/inequality comparisons → keep as {@link String}
	 *       (the column type must match what was stored, e.g. VARCHAR {@code is_laundering})</li>
	 * </ul>
	 */
	@Override
	public Object coerceParamValue(Object raw) {
		return raw; // identity — see javadoc above
	}

	/**
	 * Returns {@code true} — signals to {@code WhereClauseBuilder} that it should
	 * coerce ID-column and COUNT-comparison parameters to {@link Long} for strict Trino
	 * type matching on BIGINT columns.
	 */
	@Override
	public boolean requiresExplicitNumericIdParams() {
		return true;
	}
}
