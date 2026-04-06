package com.graphqueryengine.query.translate.sql.model;

/** Describes the semantics of a single {@code by(...)} projection expression. */
public enum ProjectionKind {
    EDGE_PROPERTY,
    OUT_VERTEX_PROPERTY,
    IN_VERTEX_PROPERTY,
    EDGE_DEGREE,
    OUT_VERTEX_COUNT,
    IN_VERTEX_COUNT,
    OUT_NEIGHBOR_PROPERTY,
    IN_NEIGHBOR_PROPERTY,
    CHOOSE_VALUES_IS_CONSTANT,
    IDENTITY
}

