package com.graphqueryengine.query.translate.sql.dialect;

/**
 * Small SQL dialect surface used by GremlinSqlTranslator.
 * This keeps traversal planning shared while allowing backend-specific rendering.
 */
public interface SqlDialect {
    String quoteIdentifier(String identifier);

    String stringAgg(String expressionSql, String delimiter);

    /**
     * Whether this dialect supports Common Table Expressions (WITH … AS (…)).
     * Both H2 and Trino/Iceberg support CTEs, so this defaults to {@code true}.
     */
    default boolean supportsCte() { return true; }

    /**
     * Coerce a JDBC bind-parameter value to the appropriate Java type for this dialect.
     *
     * <p>The default (H2 / Standard SQL) implementation is an identity — all values
     * are kept as {@link String} because H2 is permissive about implicit VARCHAR↔numeric
     * coercions in JDBC.
     *
     * @param raw the raw parameter value produced by {@link com.graphqueryengine.query.translate.sql.HasFilter#typedValue()}
     * @return the value to actually bind in the prepared statement
     */
    default Object coerceParamValue(Object raw) {
        return raw;   // H2: keep as-is
    }

    /**
     * Whether this dialect requires a {@code LIMIT} clause inside derived-table
     * subqueries (e.g. degree-aggregation LEFT JOINs) to prevent the query planner
     * from re-correlating the aggregate back into a per-row scan.
     *
     * <p>H2's query planner can re-correlate a {@code GROUP BY} subquery if there is no
     * {@code LIMIT}, turning an O(1) hash-aggregate into an O(N) per-row scan.
     * Adding {@code LIMIT 2147483647} prevents this.
     *
     * <p>Trino/Iceberg does <em>not</em> support {@code LIMIT} inside a derived table
     * used in a {@code FROM}/{@code JOIN} clause — the statement is rejected with
     * "Schema must be specified when session schema is not set".  Trino always chooses
     * a hash-aggregate plan and does not need the hint.
     *
     * @return {@code true} for H2 (default), {@code false} for Trino/Iceberg
     */
    default boolean requiresDerivedTableLimit() {
        return true;
    }

    /**
     * Whether this dialect requires BIGINT/INTEGER id columns and COUNT comparisons
     * to be bound as {@link Long} rather than {@link String}.
     *
     * <p>Trino/Iceberg enforces strict type matching: binding a {@link String} {@code "1"}
     * to a {@code BIGINT} column fails with "Cannot apply operator: bigint = varchar".
     * This flag tells {@code WhereClauseBuilder} to coerce id-column and count-comparison
     * parameters to {@link Long} at the specific sites where the column type is known
     * to be numeric, while leaving all other equality params as {@link String}
     * (e.g. VARCHAR columns like {@code is_laundering} that store {@code "0"/"1"}).
     *
     * @return {@code false} by default (H2 handles implicit coercion)
     */
    default boolean requiresExplicitNumericIdParams() {
        return false;
    }
}
