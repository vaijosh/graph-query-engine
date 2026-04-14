package com.graphqueryengine.query.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

/**
 * Response body for {@code POST /query/explain}.
 *
 * <ul>
 *   <li>{@code mode} / {@code executionEngine} — always {@code "SQL"}.
 *       The engine translates Gremlin to SQL; there is no separate "WCOJ engine".</li>
 *   <li>{@code executionStrategy} — optional human-readable note describing the
 *       internal optimisation path.  For multi-hop queries with {@code WCOJ_ENABLED=true}
 *       (the default) this explains that the Leapfrog Trie Join optimiser will be used
 *       instead of running the N-JOIN SQL plan directly.</li>
 *   <li>{@code translatedSql} — the SQL the translator produced.  For hop queries with
 *       WCOJ active this is shown for reference only; the optimiser bypasses it.</li>
 * </ul>
 */
public record QueryExplanation(
        String gremlin,
        String translatedSql,
        List<Object> parameters,
        /** Always {@code "SQL"}. */
        String mode,
        String note,
        @JsonInclude(JsonInclude.Include.NON_NULL) QueryPlan plan,
        /** Always {@code "SQL"}. Retained for API backward-compatibility. */
        @JsonInclude(JsonInclude.Include.NON_NULL) String executionEngine,
        /** Null for non-hop queries; describes WCOJ optimisation path for hop queries. */
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
