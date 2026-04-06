package com.graphqueryengine.query.translate.sql.model;

/** An alias selected by a {@code select(...).by('property')} step. */
public record SelectField(String alias, String property) {}

