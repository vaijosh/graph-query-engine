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

    // ── Predicate operator names (as used in Gremlin expressions) ─────────────
    public static final String PRED_GT  = "gt";
    public static final String PRED_GTE = "gte";
    public static final String PRED_LT  = "lt";
    public static final String PRED_LTE = "lte";
    public static final String PRED_NEQ = "neq";
    public static final String PRED_EQ  = "eq";

    // ── By-modulator sub-step tokens ──────────────────────────────────────────
    /** The {@code values} token used as a by-modulator argument prefix (e.g. {@code by(values, desc)}). */
    public static final String VALUES_TOKEN         = "values";
    public static final String VALUES_ANON_TOKEN    = "__.values()";
    /** The {@code Order.desc} token variant recognised in by() arguments. */
    public static final String ORDER_BY_DESC_TOKEN  = "Order.desc";

    // ── select() context tokens ───────────────────────────────────────────────
    public static final String SELECT_OPEN  = "select(";
    public static final String ORDER_DOT    = "Order.";

    // ── Boolean literal tokens ────────────────────────────────────────────────
    public static final String BOOL_TRUE  = "true";
    public static final String BOOL_FALSE = "false";
}
