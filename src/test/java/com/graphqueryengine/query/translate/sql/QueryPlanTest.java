package com.graphqueryengine.query.translate.sql;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.api.QueryPlan;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.translate.sql.dialect.StandardSqlDialect;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for QueryPlan output — the planner-stage debug information.
 * Tests verify that {@link GremlinSqlTranslator#translateWithPlan} produces
 * structured plan JSON showing all mapping decisions before SQL rendering.
 */
public class QueryPlanTest {
    private MappingConfig mappingConfig;
    private GremlinSqlTranslator translator;

    @BeforeEach
    public void setUp() {
        VertexMapping accountMapping = new VertexMapping(
                "account",
                "id",
                Map.of("name", "acct_name", "type", "acct_type")
        );
        VertexMapping bankMapping = new VertexMapping(
                "bank",
                "id",
                Map.of("name", "bank_name", "code", "swift_code")
        );
        EdgeMapping transferMapping = new EdgeMapping(
                "transfer",
                "id",
                "from_account",
                "to_account",
                Map.of("amount", "transfer_amount")
        );

        mappingConfig = new MappingConfig(
                Map.of("Account", accountMapping, "Bank", bankMapping),
                Map.of("TRANSFER", transferMapping)
        );

        translator = new GremlinSqlTranslator(new StandardSqlDialect());
    }

    @Test
    public void testSimpleVertexQueryPlan() {
        TranslationResult result = translator.translateWithPlan(
                "g.V().hasLabel('Account').has('name','alice')",
                mappingConfig
        );

        assertNotNull(result.plan(), "Plan should not be null when translateWithPlan is used");
        QueryPlan plan = result.plan();

        assertEquals("vertex", plan.rootType());
        assertEquals("Account", plan.rootLabel());
        assertEquals("account", plan.rootTable());
        assertEquals("StandardSqlDialect", plan.dialect());

        assertNotNull(plan.filters());
        assertEquals(1, plan.filters().size());
        assertEquals("name", plan.filters().get(0).property());

        assertNull(plan.hops(), "Simple vertex query should have no hops");
        assertNull(plan.aggregation(), "No aggregation in simple query");
    }

    @Test
    public void testHopTraversalPlan() {
        TranslationResult result = translator.translateWithPlan(
                "g.V().hasLabel('Account').out('TRANSFER').values('name')",
                mappingConfig
        );

        assertNotNull(result.plan(), "Plan should include hop information");
        QueryPlan plan = result.plan();

        assertEquals("vertex", plan.rootType());
        assertEquals("Account", plan.rootLabel());

        assertNotNull(plan.hops());
        assertEquals(1, plan.hops().size());
        QueryPlan.HopPlan hop = plan.hops().get(0);
        assertEquals("out", hop.direction());
        assertEquals(List.of("TRANSFER"), hop.labels());
        assertEquals("transfer", hop.resolvedEdgeTable());

        assertEquals("name", plan.valuesProperty());
    }

    @Test
    public void testCountAggregationPlan() {
        TranslationResult result = translator.translateWithPlan(
                "g.V().hasLabel('Account').count()",
                mappingConfig
        );

        assertNotNull(result.plan());
        QueryPlan plan = result.plan();

        assertEquals("count", plan.aggregation());
        assertNull(plan.aggregationProperty(), "count() has no property");
    }

    @Test
    public void testRootIdFilterPlan() {
        TranslationResult result = translator.translateWithPlan(
                "g.V().hasLabel('Account').has('id', 42)",
                mappingConfig
        );

        assertNotNull(result.plan());
        QueryPlan plan = result.plan();

        assertEquals("Account", plan.rootLabel());
        assertEquals("42", plan.rootIdFilter(), "Root ID filter should be captured from has('id', ...)");
    }

    @Test
    public void testProjectionPlan() {
        TranslationResult result = translator.translateWithPlan(
                "g.V().hasLabel('Account').project('name','type').by('name').by('type')",
                mappingConfig
        );

        assertNotNull(result.plan());
        QueryPlan plan = result.plan();

        assertNotNull(plan.projections());
        assertEquals(2, plan.projections().size());
        assertEquals("name", plan.projections().get(0).alias());
        assertEquals("type", plan.projections().get(1).alias());
    }

    @Test
    public void testLimitAndOrderPlan() {
        TranslationResult result = translator.translateWithPlan(
                "g.V().hasLabel('Account').order().by('name').limit(10)",
                mappingConfig
        );

        assertNotNull(result.plan());
        QueryPlan plan = result.plan();

        assertEquals("name", plan.orderBy());
        assertEquals("ASC", plan.orderDirection());
        // Limit after order on a non-hop query is recorded as preHopLimit
        assertTrue(plan.preHopLimit() != null && plan.preHopLimit() == 10,
                   "Expected limit of 10 to be recorded");
    }

    @Test
    public void testDedupAndSimplePathPlan() {
        TranslationResult result = translator.translateWithPlan(
                "g.V().hasLabel('Account').out('TRANSFER').simplePath().dedup()",
                mappingConfig
        );

        assertNotNull(result.plan());
        QueryPlan plan = result.plan();

        assertTrue(plan.simplePath(), "simplePath() should be recorded");
        assertTrue(plan.dedup(), "dedup() should be recorded");
    }

    @Test
    public void testPlanWithoutPlanResultIsNull() {
        // When using plain translate() without plan, plan field should be null
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account')",
                mappingConfig
        );

        assertNull(result.plan(), "Plain translate() should not attach a plan");
    }

    @Test
    public void testSqlStillGeneratedWithPlan() {
        TranslationResult result = translator.translateWithPlan(
                "g.V().hasLabel('Account').count()",
                mappingConfig
        );

        assertNotNull(result.sql(), "SQL should still be generated when plan is requested");
        assertTrue(result.sql().contains("COUNT"), "SQL should contain aggregation");
        assertNotNull(result.parameters(), "Parameters should be present");
    }

    @Test
    public void testGroupCountPlan() {
        TranslationResult result = translator.translateWithPlan(
                "g.V().hasLabel('Account').groupCount().by('type')",
                mappingConfig
        );

        assertNotNull(result.plan());
        QueryPlan plan = result.plan();

        assertEquals("groupCount", plan.aggregation());
        assertEquals("type", plan.aggregationProperty());
    }

    @Test
    public void testAsAliasPlan() {
        TranslationResult result = translator.translateWithPlan(
                "g.V().hasLabel('Account').as('src').out('TRANSFER').as('tgt')",
                mappingConfig
        );

        assertNotNull(result.plan());
        QueryPlan plan = result.plan();

        assertNotNull(plan.asAliases());
        assertEquals(2, plan.asAliases().size());
        assertEquals("src", plan.asAliases().get(0).label());
        assertEquals(0, plan.asAliases().get(0).hopIndexAfter());
        assertEquals("tgt", plan.asAliases().get(1).label());
        assertEquals(1, plan.asAliases().get(1).hopIndexAfter());
    }

    @Test
    public void testEdgeQueryPlan() {
        TranslationResult result = translator.translateWithPlan(
                "g.E().hasLabel('TRANSFER').has('amount', 100)",
                mappingConfig
        );

        assertNotNull(result.plan());
        QueryPlan plan = result.plan();

        assertEquals("edge", plan.rootType());
        assertEquals("TRANSFER", plan.rootLabel());
        assertEquals("transfer", plan.rootTable());

        assertNotNull(plan.filters());
        assertEquals(1, plan.filters().size());
        assertEquals("amount", plan.filters().get(0).property());
    }

    @Test
    public void testDialectInPlan() {
        // Verify that the dialect name is recorded in the plan
        TranslationResult result = translator.translateWithPlan(
                "g.V().hasLabel('Account')",
                mappingConfig
        );

        assertNotNull(result.plan());
        assertEquals("StandardSqlDialect", result.plan().dialect());
    }
}

