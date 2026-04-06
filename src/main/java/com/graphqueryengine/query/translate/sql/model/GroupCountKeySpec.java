package com.graphqueryengine.query.translate.sql.model;

/** The alias + property key for an alias-keyed {@code groupCount().by(select('alias').by('prop'))}. */
public record GroupCountKeySpec(String alias, String property) {}

