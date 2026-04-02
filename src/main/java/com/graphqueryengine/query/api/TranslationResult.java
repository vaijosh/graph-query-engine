package com.graphqueryengine.query.api;

import java.util.List;

public record TranslationResult(String sql, List<Object> parameters) {
}

