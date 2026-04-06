package com.graphqueryengine.query.translate.sql.model;
/** The kind of a {@code where()} predicate. */
public enum WhereKind {
    NEQ_ALIAS, EQ_ALIAS, PROJECT_GT, PROJECT_GTE,
    EDGE_EXISTS, OUT_NEIGHBOR_HAS, IN_NEIGHBOR_HAS,
    AND, OR, NOT
}
