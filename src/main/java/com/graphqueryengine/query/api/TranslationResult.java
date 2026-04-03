package com.graphqueryengine.query.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

public record TranslationResult(
        String sql,
        List<Object> parameters,
        @JsonInclude(JsonInclude.Include.NON_NULL) QueryPlan plan
) {
    /** Backward-compatible constructor used by all existing call sites. */
    public TranslationResult(String sql, List<Object> parameters) {
        this(sql, parameters, null);
    }
}
