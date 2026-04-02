package com.graphqueryengine.query.parser.model;

import java.util.List;

/**
 * One Gremlin step in traversal order, excluding the root step once normalized.
 */
public record GremlinStep(String name, List<String> args, String rawArgs) {
}

