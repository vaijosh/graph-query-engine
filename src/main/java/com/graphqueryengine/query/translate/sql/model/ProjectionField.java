package com.graphqueryengine.query.translate.sql.model;

/** A single {@code project('alias').by(...)} field descriptor. */
public record ProjectionField(String alias, ProjectionKind kind, String property) {}

