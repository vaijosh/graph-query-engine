package com.graphqueryengine.mapping;

import java.util.Collections;
import java.util.Map;

public record EdgeMapping(
        String table,
        String idColumn,
        String outColumn,
        String inColumn,
        Map<String, String> properties
) {
    public EdgeMapping {
        properties = properties == null ? Collections.emptyMap() : properties;
    }
}

