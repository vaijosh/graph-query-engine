package com.graphqueryengine.query;

import java.util.List;

public record QueryExplanation(
        String gremlin,
        String translatedSql,
        List<Object> parameters,
        String mode,
        String note
) {
}

