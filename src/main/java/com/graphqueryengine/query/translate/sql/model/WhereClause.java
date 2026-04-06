package com.graphqueryengine.query.translate.sql.model;

import com.graphqueryengine.query.translate.sql.HasFilter;

import java.util.List;

/**
 * A parsed {@code where()} predicate clause.
 *
 * <p>For compound predicates ({@link WhereKind#AND}, {@link WhereKind#OR}, {@link WhereKind#NOT})
 * the {@code clauses} list holds the inner sub-predicates.  For simple predicates the
 * {@code filters} list holds any {@code has()} conditions embedded in the predicate.
 */
public record WhereClause(
        WhereKind kind,
        String left,
        String right,
        List<HasFilter> filters,
        List<WhereClause> clauses) {

    /** Convenience constructor for non-compound predicates (no nested clauses). */
    public WhereClause(WhereKind kind, String left, String right, List<HasFilter> filters) {
        this(kind, left, right, filters, List.of());
    }
}

