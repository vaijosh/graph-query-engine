package com.graphqueryengine.query.translate.sql;

/**
 * A single {@code has(property, value)} or {@code has(property, predicate(value))} filter.
 *
 * <p>The {@code operator} field holds the SQL comparison operator:
 * <ul>
 *   <li>{@code "="} — equality (default, from {@code has('prop','val')})</li>
 *   <li>{@code ">"} — from {@code gt(val)}</li>
 *   <li>{@code ">="} — from {@code gte(val)}</li>
 *   <li>{@code "<"} — from {@code lt(val)}</li>
 *   <li>{@code "<="} — from {@code lte(val)}</li>
 *   <li>{@code "!="} — from {@code neq(val)}</li>
 * </ul>
 */
public record HasFilter(String property, String value, String operator) {

    /** Convenience constructor for equality filters (the common case). */
    public HasFilter(String property, String value) {
        this(property, value, "=");
    }
}

