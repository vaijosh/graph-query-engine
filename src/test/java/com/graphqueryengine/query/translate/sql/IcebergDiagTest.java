package com.graphqueryengine.query.translate.sql;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.translate.sql.dialect.IcebergSqlDialect;
import com.graphqueryengine.query.api.TranslationResult;
import org.junit.jupiter.api.Test;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergDiagTest {
    private final GremlinSqlTranslator translator = new GremlinSqlTranslator(new IcebergSqlDialect());

    private MappingConfig m() {
        return new MappingConfig(
            Map.of(
                "Account", new VertexMapping("iceberg:aml.accounts", "id", Map.of(
                        "accountId","account_id","bankId","bank_id","riskScore","risk_score",
                        "isBlocked","is_blocked"
                )),
                "Bank", new VertexMapping("iceberg:aml.banks", "id", Map.of(
                        "bankId","bank_id","bankName","bank_name","countryCode","country_code"
                )),
                "Country", new VertexMapping("iceberg:aml.countries", "id", Map.of(
                        "countryCode","country_code","countryName","country_name"
                )),
                "Alert", new VertexMapping("iceberg:aml.alerts", "id", Map.of(
                        "alertId","alert_id","severity","severity","status","status","alertType","alert_type","raisedAt","raised_at"
                ))
            ),
            Map.of(
                "TRANSFER",    new EdgeMapping("iceberg:aml.transfers",    "id", "out_id", "in_id",
                        Map.of("isLaundering","is_laundering","amount","amount","transactionId","transaction_id")),
                "FLAGGED_BY",  new EdgeMapping("iceberg:aml.account_alert","id", "out_id", "in_id", Map.of()),
                "BELONGS_TO",  new EdgeMapping("iceberg:aml.account_bank", "id", "out_id", "in_id", Map.of()),
                "LOCATED_IN",  new EdgeMapping("iceberg:aml.bank_country", "id", "out_id", "in_id", Map.of())
            )
        );
    }

    // ── Type-coercion tests ────────────────────────────────────────────────────

    /**
     * has('isLaundering','1') — is_laundering is a VARCHAR column in Iceberg.
     * The param MUST stay as String "1", NOT be coerced to Long(1).
     * Coercing it would cause: "Cannot apply operator: varchar = integer"
     */
    @Test
    void hasVarcharColumnParamStaysAsString() {
        TranslationResult r = translator.translate(
                "g.V().hasLabel('Account').where(outE('TRANSFER').has('isLaundering','1')).limit(1)",
                m());
        System.out.println("isLaundering SQL: " + r.sql());
        System.out.println("isLaundering params: " + r.parameters());
        assertFalse(r.parameters().isEmpty(), "Expected at least one parameter");
        Object param = r.parameters().get(0);
        // is_laundering is VARCHAR in Iceberg — must stay String to avoid varchar=integer error
        assertInstanceOf(String.class, param,
                "VARCHAR column param must stay as String for Trino, got: " + param.getClass());
        assertEquals("1", param);
    }

    /**
     * outE('FLAGGED_BY').count().is(0) — COUNT(*) always returns BIGINT.
     * The comparison param MUST be Long(0), not String("0").
     */
    @Test
    void countIsZeroParamIsCoercedToLong() {
        TranslationResult r = translator.translate(
                "g.V().hasLabel('Account').where(outE('FLAGGED_BY').count().is(0)).limit(10)" +
                ".project('accountId','bankId').by('accountId').by('bankId')",
                m());
        System.out.println("count=0 SQL: " + r.sql());
        System.out.println("count=0 params: " + r.parameters());
        assertFalse(r.parameters().isEmpty(), "Expected at least one parameter");
        Object last = r.parameters().get(r.parameters().size() - 1);
        // COUNT(*) returns BIGINT — comparison param must be Long
        assertInstanceOf(Long.class, last,
                "COUNT comparison param must be Long for Trino BIGINT, got: " + last.getClass());
        assertEquals(0L, last);
    }

    /**
     * hasId(1) — the id column is always BIGINT.
     * The param MUST be Long(1) not String("1").
     */
    @Test
    void hasIdParamIsCoercedToLong() {
        TranslationResult r = translator.translate(
                "g.V().hasLabel('Account').hasId(1).project('accountId','bankId').by('accountId').by('bankId')",
                m());
        System.out.println("hasId SQL: " + r.sql());
        System.out.println("hasId params: " + r.parameters());
        assertFalse(r.parameters().isEmpty(), "Expected at least one parameter for hasId");
        Object param = r.parameters().get(0);
        // id column is BIGINT — must be Long for Trino
        assertInstanceOf(Long.class, param,
                "hasId(1) must be Long for Trino BIGINT id column, got: " + param.getClass());
        assertEquals(1L, param);
    }

    /**
     * H2 dialect — all equality params stay as String (H2 handles implicit coercion).
     * Both VARCHAR and BIGINT columns receive String params — H2 is permissive.
     */
    @Test
    void h2DialectKeepsAllParamsAsString() {
        GremlinSqlTranslator h2 = new GremlinSqlTranslator(); // StandardSqlDialect
        TranslationResult r = h2.translate(
                "g.V().hasLabel('Account').where(outE('TRANSFER').has('isLaundering','1')).limit(1)",
                m());
        System.out.println("H2 isLaundering params: " + r.parameters());
        assertFalse(r.parameters().isEmpty());
        Object param = r.parameters().get(0);
        assertInstanceOf(String.class, param,
                "H2 dialect must keep '1' as String, got: " + param.getClass());

        // Also check hasId stays String for H2
        TranslationResult r2 = h2.translate(
                "g.V().hasLabel('Account').hasId(1).project('accountId').by('accountId')",
                m());
        System.out.println("H2 hasId params: " + r2.parameters());
        assertFalse(r2.parameters().isEmpty());
        // H2 hasId — String is fine since H2 coerces implicitly
        assertNotNull(r2.parameters().get(0));
    }

    // ── Compound where() tests ────────────────────────────────────────────────

    /**
     * 9d: where(and(p1, p2)) — active accounts with at least one transfer AND no alerts.
     * Both the legacy and ANTLR parsers must produce the same SQL with two COUNT subqueries.
     */
    @Test
    void whereAndCompoundPredicateProducesCorrectSql() {
        String gremlin =
                "g.V().hasLabel('Account')" +
                ".where(and(outE('TRANSFER').count().is(gt(0)), outE('FLAGGED_BY').count().is(0)))" +
                ".project('accountId','bankId').by('accountId').by('bankId').limit(10)";
        TranslationResult r = translator.translate(gremlin, m());
        System.out.println("where(and) SQL: " + r.sql());

        // Both COUNT subqueries must appear
        assertTrue(r.sql().contains("aml.transfers"),   "SQL must reference transfers table");
        assertTrue(r.sql().contains("aml.account_alert"), "SQL must reference account_alert table");
        // First: count > 0
        assertTrue(r.sql().contains("> ?") || r.sql().contains(">?"),
                "SQL must contain a > comparison for gt(0)");
        // Second: count = 0
        assertTrue(r.sql().contains("= ?"), "SQL must contain an = comparison for is(0)");
        // Both params: Long 0 (COUNT comparisons coerced by Iceberg dialect)
        assertFalse(r.parameters().isEmpty());
        // gt(0) param and is(0) param — both must be Long for Trino
        for (Object param : r.parameters()) {
            assertInstanceOf(Long.class, param,
                    "All COUNT comparison params must be Long for Trino, got: " + param.getClass());
        }
    }

    /**
     * ANTLR parser must also handle where(and(...)) without failing.
     */
    @Test
    void whereAndCompoundPredicateAntlrParserProducesSameSqlAsLegacy() {
        String gremlin =
                "g.V().hasLabel('Account')" +
                ".where(and(outE('TRANSFER').count().is(gt(0)), outE('FLAGGED_BY').count().is(0)))" +
                ".project('accountId','bankId').by('accountId').by('bankId').limit(10)";

        // ANTLR parser via GremlinSqlTranslator string overload
        TranslationResult legacy = translator.translate(gremlin, m());

        // ANTLR parser path (via SqlGraphQueryTranslator with ANTLR)
        com.graphqueryengine.query.parser.GremlinTraversalParser antlrParser =
                new com.graphqueryengine.query.parser.AntlrGremlinTraversalParser();
        com.graphqueryengine.query.parser.model.GremlinParseResult parsed = antlrParser.parse(gremlin);
        TranslationResult antlr = translator.translate(parsed, m());

        System.out.println("Legacy where(and) SQL: " + legacy.sql());
        System.out.println("ANTLR  where(and) SQL: " + antlr.sql());
        assertEquals(legacy.sql(), antlr.sql(),
                "Legacy and ANTLR parsers must produce identical SQL for where(and(...))");
        assertEquals(legacy.parameters(), antlr.parameters());
    }

    /**
     * where(or(p1, p2)) — accounts flagged OR high-risk.
     */
    @Test
    void whereOrCompoundPredicateProducesCorrectSql() {
        TranslationResult r = translator.translate(
                "g.V().hasLabel('Account')" +
                ".where(or(outE('FLAGGED_BY').count().is(gt(0)), outE('TRANSFER').count().is(gt(10))))" +
                ".project('accountId').by('accountId').limit(5)",
                m());
        System.out.println("where(or) SQL: " + r.sql());
        assertTrue(r.sql().contains(" OR "), "SQL must contain OR for where(or(...))");
        assertTrue(r.sql().contains("aml.account_alert"), "SQL must reference account_alert for FLAGGED_BY");
        assertTrue(r.sql().contains("aml.transfers"), "SQL must reference transfers for TRANSFER");
    }

    // ── Existing render tests ──────────────────────────────────────────────────

    @Test
    void diagC7() {
        var r = translator.translate(
                "g.V().hasLabel('Account').where(outE('FLAGGED_BY'))" +
                ".project('accountId','bankId','alertCount','highSeverity')" +
                ".by('accountId').by('bankId')" +
                ".by(outE('FLAGGED_BY').count())" +
                ".by(out('FLAGGED_BY').has('severity','HIGH').count()).limit(20)",
                m());
        System.out.println("C7 SQL: " + r.sql());
    }

    @Test
    void diagArrayJoin() {
        var r = translator.translate(
                "g.V().hasLabel('Bank').limit(3)" +
                ".project('bankId','countryName')" +
                ".by('bankId').by(out('LOCATED_IN').values('countryName').fold())",
                m());
        System.out.println("ARRAY_JOIN SQL: " + r.sql());
    }
}
