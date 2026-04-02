package com.graphqueryengine.query.factory;

import com.graphqueryengine.query.api.GraphQueryTranslator;

/**
 * Factory for selecting a query translator implementation.
 */
public interface GraphQueryTranslatorFactory {
	GraphQueryTranslator create();
}


