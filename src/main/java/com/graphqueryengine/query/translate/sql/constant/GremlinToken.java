package com.graphqueryengine.query.translate.sql.constant;

/**
 * Gremlin step-name and direction token constants.
 *
 * <p>These strings appear both in the parser (step dispatch) and in the SQL builders
 * (direction branching).  Centralising them makes renaming and mistype-detection trivial.
 */
public final class GremlinToken {

    private GremlinToken() {}

    // ── Traversal directions ──────────────────────────────────────────────────
    public static final String OUT   = "out";
    public static final String IN    = "in";
    public static final String BOTH  = "both";
    public static final String OUT_E = "outE";
    public static final String IN_E  = "inE";
    public static final String BOTH_E = "bothE";
    public static final String OUT_V = "outV";
    public static final String IN_V  = "inV";
    public static final String BOTH_V = "bothV";

    // ── Special filter property names ─────────────────────────────────────────
    public static final String PROP_ID   = "id";
    public static final String PROP_OUT_V = "outV";
    public static final String PROP_IN_V  = "inV";
    public static final String PROP_COUNT = "count";

    // ── Sort directions (Gremlin token names) ─────────────────────────────────
    public static final String ORDER_DESC       = "desc";
    public static final String ORDER_DESC_FULL  = "order.desc";

    // ── Aggregation step names ────────────────────────────────────────────────
    public static final String AGG_COUNT       = "count";
    public static final String AGG_SUM         = "sum";
    public static final String AGG_MEAN        = "mean";
    public static final String AGG_GROUP_COUNT = "groupCount";

    // ── Root query type labels ────────────────────────────────────────────────
    public static final String TYPE_VERTEX = "vertex";
    public static final String TYPE_EDGE   = "edge";

    // ── Projection / identity ─────────────────────────────────────────────────
    public static final String IDENTITY          = "identity()";
    public static final String IDENTITY_PREFIXED = "__.identity()";
}

