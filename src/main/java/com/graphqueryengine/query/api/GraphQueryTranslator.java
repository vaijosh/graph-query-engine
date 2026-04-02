package com.graphqueryengine.query.api;

import com.graphqueryengine.mapping.MappingConfig;

/**
 * Facade contract for translating Gremlin text into a backend-native query representation.
 * Implementations may choose parser mode (legacy/antlr) before compilation.
 */
public interface GraphQueryTranslator {
    TranslationResult translate(String gremlin, MappingConfig mappingConfig);
}


