package com.graphqueryengine.query.api;

import java.util.List;

public record QueryExplanation(
        String gremlin,
        String translatedSql,
        List<Object> parameters,
        String mode,
        String note
) {
}


