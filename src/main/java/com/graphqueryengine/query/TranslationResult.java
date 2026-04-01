package com.graphqueryengine.query;

import java.util.List;

public record TranslationResult(String sql, List<Object> parameters) {
}

