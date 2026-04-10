package com.graphqueryengine.query.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

public record QueryExplanation(
        String gremlin,
        String translatedSql,
        List<Object> parameters,
        String mode,
        String note,
        @JsonInclude(JsonInclude.Include.NON_NULL) QueryPlan plan,
        @JsonInclude(JsonInclude.Include.NON_NULL) String executionEngine,
        @JsonInclude(JsonInclude.Include.NON_NULL) String executionStrategy
) {
    /** Backward-compatible constructor for existing call sites (no plan). */
    public QueryExplanation(String gremlin, String translatedSql,
                             List<Object> parameters, String mode, String note) {
        this(gremlin, translatedSql, parameters, mode, note, null, null, null);
    }

    /** Constructor with plan but no execution engine info (legacy). */
    public QueryExplanation(String gremlin, String translatedSql,
                             List<Object> parameters, String mode, String note,
                             QueryPlan plan) {
        this(gremlin, translatedSql, parameters, mode, note, plan, null, null);
    }
}
