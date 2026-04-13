package com.graphqueryengine.query.translate.sql.constant;

/**
 * Atomic micro-fragment string constants for SQL structural punctuation and alias tokens.
 *
 * <p>Every constant here is an <em>indivisible</em> token — a single punctuation mark,
 * a SQL keyword word, a fixed alias name, or a minimal prefix.
 * Compound expressions (e.g. {@code ) AS count}) are formed by combining these atoms
 * in the calling code, giving each site full transparency about what SQL it produces.
 *
 * <h3>Naming conventions</h3>
 * <ul>
 *   <li><b>OPEN_PAREN / CLOSE_PAREN</b> — {@code (} / {@code )}</li>
 *   <li><b>DOT</b> — column-qualifier separator {@code .}</li>
 *   <li><b>SPACE</b> — single space used to separate tokens</li>
 *   <li><b>AS</b> — the SQL {@code AS} keyword (space-padded)</li>
 *   <li><b>ALIAS_*</b> — a fixed, named SQL alias (no surrounding spaces)</li>
 *   <li><b>PREFIX_*</b> — an alias followed by {@code .} for column qualification</li>
 * </ul>
 */
public final class SqlFragment {

    private SqlFragment() {}

    // ── Punctuation ───────────────────────────────────────────────────────────
    public static final String OPEN_PAREN   = "(";
    public static final String CLOSE_PAREN  = ")";
    public static final String DOT          = ".";
    public static final String COMMA_SPACE  = ", ";
    public static final String SPACE        = " ";
    public static final String UNDERSCORE   = "_";
    public static final String COLON        = ":";
    public static final String EQUALS_SIGN  = "=";

    // ── SQL string-literal quoting ─────────────────────────────────────────────
    /** Single-quote character used to delimit SQL string literals: {@code '}. */
    public static final String SQL_QUOTE          = "'";
    /** Escaped single-quote inside a SQL string literal: {@code ''}. */
    public static final String SQL_ESCAPED_QUOTE  = "''";
    /** Double-quote character used to delimit Gremlin string literals: {@code "}. */
    public static final String DOUBLE_QUOTE       = "\"";

    // ── Comparison connectors ─────────────────────────────────────────────────
    /** Column equality expression with surrounding spaces: {@code  = }. */
    public static final String SPACE_EQ_SPACE = " = ";

    // ── Aggregate alias names (unquoted, lower-case, matching COUNT_STAR) ─────
    /** The alias name emitted by {@code COUNT(*) AS count}: {@code count}. */
    public static final String ALIAS_COUNT = "count";
    /** The alias name emitted by {@code SUM(…) AS sum}: {@code sum}. */
    public static final String ALIAS_SUM   = "sum";
    /** The alias name emitted by {@code AVG(…) AS mean}: {@code mean}. */
    public static final String ALIAS_MEAN  = "mean";

    // ── Join subquery internal aliases ─────────────────────────────────────────
    /** Edge subquery alias used inside neighbor-has EXISTS: {@code _we}. */
    public static final String ALIAS_WE         = "_we";
    /** Vertex subquery alias used inside neighbor-has EXISTS: {@code _wv}. */
    public static final String ALIAS_WV         = "_wv";
    /** Correlated-exists edge alias: {@code we}. */
    public static final String ALIAS_WE_CORR    = "we";
    /** Edge subquery alias prefix for neighbor-agg subqueries: {@code _nje}. */
    public static final String ALIAS_NJE_PREFIX = "_nje";
    /** Vertex subquery alias prefix for neighbor-agg subqueries: {@code _njv}. */
    public static final String ALIAS_NJV_PREFIX = "_njv";
    /** Edge sub-table alias name (used in subquery FROM): {@code _e}. */
    public static final String ALIAS_SUB_E      = "_e";
    /** Vertex sub-table alias name (used in subquery FROM): {@code _tv}. */
    public static final String ALIAS_SUB_TV     = "_tv";
    /** Qualified column prefix for _e alias: {@code _e.}. Composed as ALIAS_SUB_E + DOT. */
    public static final String PREFIX_E         = "_e.";

    // ── Hop-query structural aliases ──────────────────────────────────────────
    /** Wildcard star token: {@code *}. */
    public static final String STAR      = "*";
    /** Root-vertex alias: {@code v0}. */
    public static final String V0        = "v0";
    /** Vertex alias prefix: {@code v}. */
    public static final String V_PREFIX  = "v";
    /** Edge alias prefix: {@code e}. */
    public static final String E_PREFIX  = "e";
    /** Vertex-target alias prefix for multi-label alt-hops: {@code vt}. */
    public static final String VT_PREFIX = "vt";

    // ── Union / subquery wrapper aliases ──────────────────────────────────────
    /** Alias name for union subquery wrappers: {@code _u}. */
    public static final String ALIAS_U   = "_u";
    /** Alias name for bothV union subquery wrappers: {@code _uv}. */
    public static final String ALIAS_UV  = "_uv";
    /** Alias name for wrapped projection outer queries: {@code p}. */
    public static final String ALIAS_P   = "p";

    // ── Edge-endpoint and projection aliases ──────────────────────────────────
    /** Vertex table alias in edge-endpoint JOINs: {@code v}. */
    public static final String ALIAS_V     = "v";
    /** Edge table alias in edge-endpoint JOINs: {@code e}. */
    public static final String ALIAS_E     = "e";
    /** Prefixed v alias: {@code v.}. */
    public static final String PREFIX_V    = "v.";
    /** Prefixed e alias: {@code e.}. */
    public static final String PREFIX_E_DOT = "e.";
    /** Prefixed ov alias: {@code ov.}. */
    public static final String PREFIX_OV   = "ov.";
    /** Prefixed iv alias: {@code iv.}. */
    public static final String PREFIX_IV   = "iv.";
    /** Out-vertex alias in edge projection JOINs: {@code ov}. */
    public static final String ALIAS_OV    = "ov";
    /** In-vertex alias in edge projection JOINs: {@code iv}. */
    public static final String ALIAS_IV    = "iv";

    // ── CTE / inline-subquery identifiers ─────────────────────────────────────
    /** CTE name used in hop queries: {@code _start}. */
    public static final String START_ALIAS = "_start";

    // ── Empty string ──────────────────────────────────────────────────────────
    /** Empty string — used as a no-op token (e.g., non-DISTINCT SELECT). */
    public static final String EMPTY = "";

    // ── Indexed hop aliases (alias only, no trailing dot) ────────────────────
    /** Root edge alias in alias-select patterns: {@code e0}. */
    public static final String ALIAS_E0 = "e0";
    /** First vertex alias in alias-select patterns: {@code v1}. */
    public static final String ALIAS_V1 = "v1";
    /** Second edge alias in alias-select patterns: {@code e2}. */
    public static final String ALIAS_E2 = "e2";
    /** Third vertex alias in alias-select patterns: {@code v3}. */
    public static final String ALIAS_V3 = "v3";

    // ── Indexed hop alias prefixes (alias + dot) ──────────────────────────────
    /** Qualified column prefix for e0 alias: {@code e0.}. */
    public static final String PREFIX_E0 = "e0.";
    /** Qualified column prefix for v1 alias: {@code v1.}. */
    public static final String PREFIX_V1 = "v1.";
    /** Qualified column prefix for e2 alias: {@code e2.}. */
    public static final String PREFIX_E2 = "e2.";
    /** Qualified column prefix for v3 alias: {@code v3.}. */
    public static final String PREFIX_V3 = "v3.";
}
