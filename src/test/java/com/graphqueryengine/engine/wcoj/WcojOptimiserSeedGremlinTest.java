package com.graphqueryengine.engine.wcoj;

import com.graphqueryengine.query.translate.sql.HasFilter;
import com.graphqueryengine.query.translate.sql.model.WhereClause;
import com.graphqueryengine.query.translate.sql.model.WhereKind;
import com.graphqueryengine.query.translate.sql.parse.GremlinStepParser;
import com.graphqueryengine.query.parser.AntlrGremlinTraversalParser;
import com.graphqueryengine.query.parser.model.GremlinParseResult;
import com.graphqueryengine.query.translate.sql.model.ParsedTraversal;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link WcojOptimiser#buildSeedGremlin} and
 * {@link WcojOptimiser#renderWhereClause}.
 *
 * <p>Regression guard: WCOJ was returning 0 results for queries such as
 * {@code g.V().hasLabel('Account').where(outE('TRANSFER').has('isLaundering','1')).limit(1).repeat(out('TRANSFER').simplePath()).times(2).path().by('accountId').limit(10)}
 * because {@code buildSeedGremlin} dropped the {@code where()} clause — so
 * the seed vertex was arbitrary (likely one with no laundering transfers) and no
 * matching paths were found.
 */
class WcojOptimiserSeedGremlinTest {

    private final GremlinStepParser stepParser = new GremlinStepParser();
    private final AntlrGremlinTraversalParser parser = new AntlrGremlinTraversalParser();

    // ── renderWhereClause ─────────────────────────────────────────────────────

    @Test
    void renderWhereClause_edgeExists_noFilters() {
        WhereClause wc = new WhereClause(WhereKind.EDGE_EXISTS, "outE", "TRANSFER", List.of());
        assertEquals("outE('TRANSFER')", WcojOptimiser.renderWhereClause(wc));
    }

    @Test
    void renderWhereClause_edgeExists_withHasFilter() {
        WhereClause wc = new WhereClause(WhereKind.EDGE_EXISTS, "outE", "TRANSFER",
                List.of(new HasFilter("isLaundering", "1")));
        assertEquals("outE('TRANSFER').has('isLaundering','1')",
                WcojOptimiser.renderWhereClause(wc));
    }

    @Test
    void renderWhereClause_edgeExists_inDirection() {
        WhereClause wc = new WhereClause(WhereKind.EDGE_EXISTS, "inE", "BELONGS_TO",
                List.of(new HasFilter("active", "true")));
        assertEquals("inE('BELONGS_TO').has('active','true')",
                WcojOptimiser.renderWhereClause(wc));
    }

    @Test
    void renderWhereClause_and_twoEdgeExists() {
        WhereClause c1 = new WhereClause(WhereKind.EDGE_EXISTS, "outE", "TRANSFER",
                List.of(new HasFilter("isLaundering", "1")));
        WhereClause c2 = new WhereClause(WhereKind.EDGE_EXISTS, "outE", "FLAGGED_BY",
                List.of());
        WhereClause wc = new WhereClause(WhereKind.AND, null, null, List.of(), List.of(c1, c2));
        assertEquals("and(outE('TRANSFER').has('isLaundering','1'), outE('FLAGGED_BY'))",
                WcojOptimiser.renderWhereClause(wc));
    }

    @Test
    void renderWhereClause_not_wrapsInner() {
        WhereClause inner = new WhereClause(WhereKind.EDGE_EXISTS, "outE", "FLAGGED_BY", List.of());
        WhereClause wc = new WhereClause(WhereKind.NOT, null, null, List.of(), List.of(inner));
        assertEquals("not(outE('FLAGGED_BY'))", WcojOptimiser.renderWhereClause(wc));
    }

    @Test
    void renderWhereClause_aliasBased_returnsNull() {
        // NEQ_ALIAS / EQ_ALIAS don't make sense in a seed sub-query; must return null
        WhereClause wc = new WhereClause(WhereKind.NEQ_ALIAS, "a", "b", List.of());
        assertNull(WcojOptimiser.renderWhereClause(wc));
    }

    @Test
    void renderWhereClause_null_returnsNull() {
        assertNull(WcojOptimiser.renderWhereClause(null));
    }

    // ── buildSeedGremlin ──────────────────────────────────────────────────────

    /**
     * Regression test: the seed Gremlin for
     * {@code g.V().hasLabel('Account').where(outE('TRANSFER').has('isLaundering','1')).limit(1)...}
     * must include the {@code where()} clause.
     */
    @Test
    void buildSeedGremlin_includesWhereEdgeExists() {
        String gremlin = "g.V().hasLabel('Account')"
                + ".where(outE('TRANSFER').has('isLaundering','1'))"
                + ".limit(1)"
                + ".repeat(out('TRANSFER').simplePath()).times(2)"
                + ".path().by('accountId').limit(10)";

        GremlinParseResult pr = parser.parse(gremlin);
        ParsedTraversal parsed = stepParser.parse(pr.steps(), pr.rootIdFilter());

        String seed = WcojOptimiser.buildSeedGremlin(parsed);

        assertTrue(seed.contains("where(outE('TRANSFER').has('isLaundering','1'))"),
                "Seed gremlin must contain the where clause but was: " + seed);
        assertTrue(seed.contains(".limit(1)"),
                "Seed gremlin must contain the preHopLimit but was: " + seed);
        assertFalse(seed.contains("repeat"),
                "Seed gremlin must NOT contain the repeat step");
    }

    @Test
    void buildSeedGremlin_noWhere_noHasFilters() {
        String gremlin = "g.V().hasLabel('Account')"
                + ".limit(1)"
                + ".repeat(out('TRANSFER').simplePath()).times(2)"
                + ".path().by('accountId').limit(10)";

        GremlinParseResult pr = parser.parse(gremlin);
        ParsedTraversal parsed = stepParser.parse(pr.steps(), pr.rootIdFilter());

        String seed = WcojOptimiser.buildSeedGremlin(parsed);
        assertEquals("g.V().hasLabel('Account').limit(1)", seed);
    }

    @Test
    void buildSeedGremlin_hasFilter_noWhere() {
        String gremlin = "g.V().hasLabel('Account')"
                + ".has('isBlocked','true')"
                + ".limit(5)"
                + ".repeat(out('TRANSFER').simplePath()).times(3)"
                + ".path().by('accountId').limit(20)";

        GremlinParseResult pr = parser.parse(gremlin);
        ParsedTraversal parsed = stepParser.parse(pr.steps(), pr.rootIdFilter());

        String seed = WcojOptimiser.buildSeedGremlin(parsed);
        assertTrue(seed.contains(".has('isBlocked','true')"),
                "Seed gremlin must include has() filter: " + seed);
        assertTrue(seed.contains(".limit(5)"),
                "Seed gremlin must include limit: " + seed);
        assertFalse(seed.contains("where"),
                "No where clause expected: " + seed);
    }

    @Test
    void buildSeedGremlin_andWhere() {
        String gremlin = "g.V().hasLabel('Account')"
                + ".where(and(outE('TRANSFER').has('isLaundering','1'), outE('FLAGGED_BY').count().is(0)))"
                + ".limit(1)"
                + ".repeat(out('TRANSFER').simplePath()).times(3)"
                + ".path().by('accountId').limit(10)";

        GremlinParseResult pr = parser.parse(gremlin);
        ParsedTraversal parsed = stepParser.parse(pr.steps(), pr.rootIdFilter());

        String seed = WcojOptimiser.buildSeedGremlin(parsed);
        // The and() clause should be rendered; EDGE_COUNT_IS maps to EDGE_EXISTS internally
        assertTrue(seed.contains("where("),
                "Seed gremlin should contain where clause: " + seed);
    }
}

