package com.graphqueryengine.query.translate.sql.constant;

/**
 * SQL comparison-operator string constants.
 *
 * <p>These are the SQL operator symbols produced when Gremlin predicates such as
 * {@code gt()}, {@code gte()}, {@code lt()}, {@code lte()}, {@code neq()}, and {@code eq()}
 * are translated to SQL.  Centralising them avoids scattered {@code ">"}, {@code ">="} etc.
 * literals throughout the code-base.
 */
public final class SqlOperator {

    private SqlOperator() {}

    public static final String EQ  = "=";
    public static final String NEQ = "!=";
    public static final String GT  = ">";
    public static final String GTE = ">=";
    public static final String LT  = "<";
    public static final String LTE = "<=";

    /** Converts a Gremlin predicate name to its SQL operator string. */
    public static String fromPredicate(String predicate) {
        return switch (predicate) {
            case GremlinToken.PRED_GT  -> GT;
            case GremlinToken.PRED_GTE -> GTE;
            case GremlinToken.PRED_LT  -> LT;
            case GremlinToken.PRED_LTE -> LTE;
            case GremlinToken.PRED_NEQ -> NEQ;
            default                    -> EQ;
        };
    }
}

