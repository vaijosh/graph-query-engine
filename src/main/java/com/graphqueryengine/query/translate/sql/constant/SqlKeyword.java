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

    // ── Set operations ────────────────────────────────────────────────────────
    public static final String UNION_ALL       = " UNION ALL ";
    public static final String UNION           = " UNION ";

    // ── Aggregate functions ───────────────────────────────────────────────────
    public static final String COUNT_STAR      = "COUNT(*) AS count";
    @SuppressWarnings("unused")
    public static final String COUNT_STAR_RAW  = "COUNT(*)";
    public static final String COUNT_DISTINCT  = "COUNT(DISTINCT ";
    public static final String SUM_ALIAS       = ") AS sum";
    public static final String MEAN_ALIAS      = ") AS mean";
    public static final String SUM_FN          = "SUM(";
    public static final String AVG_FN          = "AVG(";

    // ── Predicates / conditions ───────────────────────────────────────────────
    public static final String IS_NULL         = " IS NULL";
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

    // ── CTE ───────────────────────────────────────────────────────────────────
    public static final String WITH            = "WITH ";
    public static final String AS_OPEN         = " AS (";
    public static final String CLOSE_PAREN     = ")";

    // ── Sort directions ───────────────────────────────────────────────────────
    public static final String ASC             = "ASC";
    public static final String DESC            = "DESC";
}

