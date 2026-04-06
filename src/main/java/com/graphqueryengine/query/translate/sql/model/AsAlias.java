package com.graphqueryengine.query.translate.sql.model;

/** Holds a parsed alias label and the hop-index at which it was declared via {@code .as('label')}. */
public record AsAlias(String label, int hopIndexAfter) {}

