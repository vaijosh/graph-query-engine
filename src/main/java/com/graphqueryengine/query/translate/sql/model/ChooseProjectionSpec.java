package com.graphqueryengine.query.translate.sql.model;

/**
 * Parsed spec for a {@code choose(values('prop').is(pred(threshold)), constant(T), constant(F))}
 * projection expression.
 */
public record ChooseProjectionSpec(
        String property,
        String predicate,
        String predicateValue,
        String trueConstant,
        String falseConstant) {}

