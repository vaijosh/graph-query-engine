package com.graphqueryengine.query.parser;

import com.graphqueryengine.query.parser.model.GremlinParseResult;
import com.graphqueryengine.query.parser.model.GremlinStep;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AntlrGremlinTraversalParserTest {

    private final AntlrGremlinTraversalParser parser = new AntlrGremlinTraversalParser();

    @Test
    void rejectsMalformedGremlin() {
        assertThrows(IllegalArgumentException.class, () -> parser.validate("g.V("));
    }

    @Test
    void parsesRepeatSimplePathWithNumericAccountId() {
        // Regression: account IDs like '100428660' were causing token recognition errors
        String q = "g.V().hasLabel('Account').has('accountId','100428660')" +
                   ".repeat(out('TRANSFER').simplePath()).times(5)" +
                   ".path().by('accountId').limit(10)";
        GremlinParseResult result = parser.parse(q);
        // Dump all steps for diagnosis
        for (GremlinStep s : result.steps()) {
            System.out.println("step=" + s.name() + " args=" + s.args() + " rawArgs=[" + s.rawArgs() + "]");
        }
        // Verify the repeat rawArgs is correct
        GremlinStep repeatStep = result.steps().stream()
                .filter(s -> "repeat".equals(s.name())).findFirst().orElse(null);
        assertNotNull(repeatStep, "repeat step should exist");
        assertEquals("out('TRANSFER').simplePath()", repeatStep.rawArgs(),
                "rawArgs for repeat should be exactly: out('TRANSFER').simplePath()");
        // has step rawArgs should be the two quoted args
        GremlinStep hasStep = result.steps().stream()
                .filter(s -> "has".equals(s.name())).findFirst().orElse(null);
        assertNotNull(hasStep);
        assertTrue(hasStep.rawArgs().contains("100428660"),
                "has rawArgs should contain the account id: " + hasStep.rawArgs());
    }
}
