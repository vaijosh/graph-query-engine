package com.graphqueryengine.query.parser;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class AntlrGremlinTraversalParserTest {

    private final AntlrGremlinTraversalParser parser = new AntlrGremlinTraversalParser();

    @Test
    void rejectsMalformedGremlin() {
        assertThrows(IllegalArgumentException.class, () -> parser.validate("g.V("));
    }
}


