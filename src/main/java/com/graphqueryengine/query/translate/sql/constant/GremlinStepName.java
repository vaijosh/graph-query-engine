package com.graphqueryengine.query.translate.sql.constant;

/**
 * Gremlin step-name string constants.
 *
 * <p>Used as the dispatch keys in the {@code GremlinStepParser} switch statement and as
 * string literals elsewhere.  Centralising them prevents typos and makes global renames trivial.
 */
public final class GremlinStepName {

    private GremlinStepName() {}

    // ── Filter / label steps ──────────────────────────────────────────────────
    public static final String HAS_LABEL  = "hasLabel";
    public static final String HAS        = "has";
    public static final String HAS_ID     = "hasId";
    public static final String HAS_NOT    = "hasNot";
    public static final String IS         = "is";

    // ── Property projection steps ─────────────────────────────────────────────
    public static final String VALUES     = "values";
    public static final String VALUE_MAP  = "valueMap";

    // ── Traversal / hop steps ─────────────────────────────────────────────────
    public static final String OUT        = "out";
    public static final String IN         = "in";
    public static final String BOTH       = "both";
    public static final String OUT_V      = "outV";
    public static final String IN_V       = "inV";
    public static final String BOTH_V     = "bothV";
    public static final String OTHER_V    = "otherV";
    public static final String OUT_E      = "outE";
    public static final String IN_E       = "inE";
    public static final String BOTH_E     = "bothE";

    // ── Repeat / path steps ───────────────────────────────────────────────────
    public static final String REPEAT      = "repeat";
    public static final String TIMES       = "times";
    public static final String SIMPLE_PATH = "simplePath";
    public static final String PATH        = "path";

    // ── Limit / slice steps ───────────────────────────────────────────────────
    public static final String LIMIT       = "limit";

    // ── Aggregation steps ─────────────────────────────────────────────────────
    public static final String COUNT       = "count";
    public static final String SUM         = "sum";
    public static final String MEAN        = "mean";
    public static final String GROUP_COUNT = "groupCount";

    // ── Projection / selection steps ─────────────────────────────────────────
    public static final String PROJECT     = "project";
    public static final String BY          = "by";
    public static final String AS          = "as";
    public static final String SELECT      = "select";
    public static final String DEDUP       = "dedup";
    public static final String IDENTITY    = "identity";

    // ── Ordering / filtering steps ────────────────────────────────────────────
    public static final String ORDER       = "order";
    public static final String WHERE       = "where";

    // ── Repeat body helpers ───────────────────────────────────────────────────
    public static final String SIMPLE_PATH_SUFFIX = ".simplePath()";
}

