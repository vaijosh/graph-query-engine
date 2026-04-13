package com.graphqueryengine.query.translate.sql.constant;

/**
 * SQL keyword and fragment constants used throughout the SQL-rendering layer.
 *
 * <p>Centralising these strings ensures consistency (e.g., exact spacing),
 * makes dialect-specific overrides easy to locate, and prevents typos.
 */
public final class SqlKeyword {

    private SqlKeyword() {}

    // ── DML clauses ───────────────────────────────────────────────────────────
    public static final String SELECT          = "SELECT ";
    public static final String SELECT_DISTINCT = "SELECT DISTINCT ";
    public static final String FROM            = " FROM ";
    public static final String WHERE           = " WHERE ";
    public static final String AND             = " AND ";
    public static final String OR              = " OR ";
    public static final String NOT             = "NOT ";
    public static final String AS             = " AS ";

    // ── JOIN variants ─────────────────────────────────────────────────────────
    public static final String JOIN            = " JOIN ";
    public static final String LEFT_JOIN       = " LEFT JOIN ";
    public static final String ON              = " ON ";

    // ── Aggregation / grouping ────────────────────────────────────────────────
    public static final String GROUP_BY        = " GROUP BY ";
    public static final String ORDER_BY        = " ORDER BY ";
    public static final String LIMIT           = " LIMIT ";

    /** The count-alias name used in {@code ORDER BY count DESC}. Identical to {@link SqlFragment#ALIAS_COUNT}. */
    public static final String COUNT_ALIAS     = "count ";

    // ── Set operations ────────────────────────────────────────────────────────
    public static final String UNION_ALL       = " UNION ALL ";
    public static final String UNION           = " UNION ";

    // ── Aggregate functions ───────────────────────────────────────────────────
    /** {@code COUNT(*) AS count} — full count aggregate expression. */
    public static final String COUNT_STAR      = "COUNT(*)" + AS + SqlFragment.ALIAS_COUNT;
    @SuppressWarnings("unused")
    public static final String COUNT_STAR_RAW  = "COUNT(*)";
    public static final String COUNT_DISTINCT  = "COUNT(DISTINCT ";
    /** {@code ) AS sum} — closes a SUM(…) aggregate and aliases it. */
    public static final String SUM_ALIAS       = SqlFragment.CLOSE_PAREN + AS + SqlFragment.ALIAS_SUM;
    /** {@code ) AS mean} — closes an AVG(…) aggregate and aliases it. */
    public static final String MEAN_ALIAS      = SqlFragment.CLOSE_PAREN + AS + SqlFragment.ALIAS_MEAN;
    public static final String SUM_FN          = "SUM(";
    public static final String AVG_FN          = "AVG(";

    // ── Predicates / conditions ───────────────────────────────────────────────
    public static final String IS_NULL         = " IS NULL";
    /**
     * Operator token used in {@link com.graphqueryengine.query.translate.sql.HasFilter}
     * to signal an IS NULL check (no bind parameter needed).
     */
    public static final String IS_NULL_OP      = "IS NULL";
    public static final String EXISTS          = "EXISTS ";
    public static final String NOT_IN          = " NOT IN (";
    public static final String NEQ             = " <> ";

    // ── Misc ──────────────────────────────────────────────────────────────────
    public static final String DISTINCT        = "DISTINCT ";
    public static final String STAR            = "*";
    public static final String NULL_LITERAL    = "NULL";
    public static final String PLACEHOLDER     = "?";
    public static final String SELECT_1        = "SELECT 1 FROM ";
    public static final String SELECT_COUNT    = "SELECT COUNT(*) FROM ";
    public static final String CASE_WHEN       = "CASE WHEN ";
    public static final String THEN            = " THEN ";
    public static final String ELSE            = " ELSE ";
    public static final String END             = " END";
    /** {@code FROM (} — open a subquery/derived-table reference. */
    public static final String FROM_SUBQUERY   = " FROM (";

    // ── CTE ───────────────────────────────────────────────────────────────────
    public static final String WITH            = "WITH ";
    public static final String AS_OPEN         = " AS (";
    public static final String CLOSE_PAREN     = ")";

    // ── Sort directions ───────────────────────────────────────────────────────
    public static final String ASC             = "ASC";
    public static final String DESC            = "DESC";
}

