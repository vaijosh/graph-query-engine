package com.graphqueryengine.query.translate.sql.model;

import java.util.List;

/** A single direction step in a graph traversal (e.g., {@code out('LABEL')}). */
public record HopStep(String direction, List<String> labels) {

    public HopStep(String direction, String singleLabel) {
        this(direction, singleLabel == null ? List.of() : List.of(singleLabel));
    }

    public String singleLabel() {
        if (labels.isEmpty()) return null;
        if (labels.size() > 1) throw new IllegalStateException("singleLabel() called on multi-label HopStep: " + labels);
        return labels.get(0);
    }
}

