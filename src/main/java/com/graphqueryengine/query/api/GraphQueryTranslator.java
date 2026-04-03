package com.graphqueryengine.query.api;

import com.graphqueryengine.mapping.MappingConfig;

/**
 * Facade contract for translating Gremlin text into a backend-native query representation.
 * Implementations may choose parser mode (legacy/antlr) before compilation.
 */
public interface GraphQueryTranslator {
    /**
     * Translate a Gremlin query to SQL without plan output.
     */
    TranslationResult translate(String gremlin, MappingConfig mappingConfig);

    /**
     * Translate a Gremlin query to SQL and include a {@link QueryPlan} with the result.
     * This allows callers to inspect the planner's decisions before SQL rendering.
     * Default implementation calls {@link #translate} and does not attach a plan.
     */
    default TranslationResult translateWithPlan(String gremlin, MappingConfig mappingConfig) {
        return translate(gremlin, mappingConfig);
    }
}


