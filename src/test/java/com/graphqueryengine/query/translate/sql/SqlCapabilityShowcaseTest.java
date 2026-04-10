package com.graphqueryengine.query.translate.sql;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.api.TranslationResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SQL capability showcase scenarios with exact SQL assertions.
 * These tests cover all Gremlin steps supported in SQL-translation mode,
 * directly derived from sql_capability_showcase.ipynb.
 * Unlike the original `SqlCapabilityShowcaseTest`, this version uses exact SQL
 * comparisons (assertEquals) instead of loose `contains()` assertions,
 * providing stricter validation of the translator output.
 * Data model (AML mapping):
 *   Account --[TRANSFER]--> Account
 *   Account --[BELONGS_TO]--> Bank
 *   Account --[FLAGGED_BY]--> Alert
 *   Account --[SENT_VIA]--> Country
 *   Bank --[LOCATED_IN]--> Country
 */
class SqlCapabilityShowcaseTest {

    private final GremlinSqlTranslator translator = new GremlinSqlTranslator();

    private MappingConfig amlMapping() {
        return new MappingConfig(
                Map.of(
                        "Account", new VertexMapping("aml_accounts", "id", Map.of(
                                "accountId", "account_id",
                                "bankId", "bank_id",
                                "riskScore", "risk_score",
                                "isBlocked", "is_blocked",
                                "openedDate", "opened_date"
                        )),
                        "Bank", new VertexMapping("aml_banks", "id", Map.of(
                                "bankId", "bank_id",
                                "bankName", "bank_name",
                                "countryCode", "country_code"
                        )),
                        "Country", new VertexMapping("aml_countries", "id", Map.of(
                                "countryCode", "country_code",
                                "countryName", "country_name",
                                "riskLevel", "risk_level"
                        )),
                        "Alert", new VertexMapping("aml_alerts", "id", Map.of(
                                "alertId", "alert_id",
                                "alertType", "alert_type",
                                "severity", "severity"
                        ))
                ),
                Map.of(
                        "TRANSFER", new EdgeMapping("aml_transfers", "id", "from_account_id", "to_account_id", Map.of(
                                "amount", "amount",
                                "currency", "currency",
                                "isLaundering", "is_laundering"
                        )),
                        "BELONGS_TO", new EdgeMapping("aml_account_bank", "id", "account_id", "bank_id", Map.of(
                                "isPrimary", "is_primary"
                        )),
                        "FLAGGED_BY", new EdgeMapping("aml_account_alert", "id", "account_id", "alert_id", Map.of(
                                "flaggedAt", "flagged_at"
                        )),
                        "SENT_VIA", new EdgeMapping("aml_account_country", "id", "account_id", "country_id", Map.of()),
                        "LOCATED_IN", new EdgeMapping("aml_bank_country", "id", "bank_id", "country_id", Map.of())
                )
        );
    }

    // ============================================================================
    // Exact SQL Assertion Tests
    // ============================================================================

    @Test
    void test_exact_count_all_accounts() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').count()",
                amlMapping()
        );

        assertEquals("SELECT COUNT(*) AS count FROM aml_accounts", result.sql());
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void test_exact_count_all_transfers() {
        TranslationResult result = translator.translate(
                "g.E().hasLabel('TRANSFER').count()",
                amlMapping()
        );

        assertEquals("SELECT COUNT(*) AS count FROM aml_transfers", result.sql());
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void test_exact_has_equality_filter() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').has('isBlocked', '1').limit(10)",
                amlMapping()
        );

        assertEquals("SELECT * FROM aml_accounts WHERE is_blocked = ? LIMIT 10", result.sql());
        assertEquals(1, result.parameters().size());
        assertEquals("1", result.parameters().get(0));
    }

    @Test
    void test_exact_has_predicate_gt() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').has('riskScore', gt(70)).limit(10)",
                amlMapping()
        );

        assertEquals("SELECT * FROM aml_accounts WHERE risk_score > ? LIMIT 10", result.sql());
        assertEquals(1, result.parameters().size());
        assertEquals(70L, result.parameters().get(0));
    }

    @Test
    void test_exact_hasId() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').hasId(1572888).limit(10)",
                amlMapping()
        );

        assertEquals("SELECT * FROM aml_accounts WHERE id = ? LIMIT 10", result.sql());
        assertEquals(1, result.parameters().size());
        assertEquals("1572888", result.parameters().get(0));
    }

    @Test
    void test_exact_hasNot_null_check() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').hasNot('openedDate').limit(10)",
                amlMapping()
        );

        assertEquals("SELECT * FROM aml_accounts WHERE opened_date IS NULL LIMIT 10", result.sql());
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void test_exact_values_with_predicate() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').values('riskScore').is(gt(80)).limit(10)",
                amlMapping()
        );

        assertEquals("SELECT risk_score AS riskScore FROM aml_accounts WHERE risk_score > ? LIMIT 10", result.sql());
        assertEquals(1, result.parameters().size());
        assertEquals(80L, result.parameters().get(0));
    }

    @Test
    void test_exact_edge_with_property_filter() {
        TranslationResult result = translator.translate(
                "g.E().hasLabel('TRANSFER').has('isLaundering','1').limit(10)",
                amlMapping()
        );

        assertEquals("SELECT * FROM aml_transfers WHERE is_laundering = ? LIMIT 10", result.sql());
        assertEquals(1, result.parameters().size());
        assertEquals("1", result.parameters().get(0));
    }

    @Test
    void test_exact_outV_select_edge_source() {
        TranslationResult result = translator.translate(
                "g.E().hasLabel('TRANSFER').limit(5).outV().limit(5)",
                amlMapping()
        );

        assertEquals("SELECT DISTINCT v.* FROM aml_accounts v JOIN aml_transfers e ON v.id = e.from_account_id LIMIT 5", result.sql());
    }

    @Test
    void test_exact_inV_select_edge_destination() {
        TranslationResult result = translator.translate(
                "g.E().hasLabel('TRANSFER').limit(5).inV().limit(5)",
                amlMapping()
        );

        assertEquals("SELECT DISTINCT v.* FROM aml_accounts v JOIN aml_transfers e ON v.id = e.to_account_id LIMIT 5", result.sql());
    }

    @Test
    void test_exact_project_single_column() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').project('accountId').by('accountId').limit(10)",
                amlMapping()
        );

        assertEquals("SELECT v.account_id AS \"accountId\" FROM aml_accounts v LIMIT 10", result.sql());
    }

    @Test
    void test_exact_sum_aggregation() {
        TranslationResult result = translator.translate(
                "g.E().hasLabel('TRANSFER').values('amount').sum()",
                amlMapping()
        );

        assertEquals("SELECT SUM(amount) AS sum FROM aml_transfers", result.sql());
    }

    @Test
    void test_exact_simple_limit() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').limit(5)",
                amlMapping()
        );

        assertEquals("SELECT * FROM aml_accounts LIMIT 5", result.sql());
    }

    // ============================================================================
    // Complex Query Tests (with structural assertions where exact SQL varies)
    // ============================================================================

    @Test
    void test_structural_order_by_descending() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').order().by('riskScore', Order.desc).limit(10)",
                amlMapping()
        );

        String sql = result.sql();
        assertTrue(sql.contains("ORDER BY"));
        assertTrue(sql.contains("DESC"));
        assertTrue(sql.contains("LIMIT 10"));
    }

    @Test
    void test_structural_groupCount_aggregation() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').groupCount().by('bankId')",
                amlMapping()
        );

        String sql = result.sql();
        assertTrue(sql.contains("GROUP BY bank_id"));
        assertTrue(sql.contains("COUNT(*)"));
    }

    @Test
    void test_structural_join_out_traversal() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').limit(5).out('BELONGS_TO').limit(5)",
                amlMapping()
        );

        String sql = result.sql();
        assertTrue(sql.contains("JOIN"));
        assertTrue(sql.contains("aml_accounts"));
        assertTrue(sql.contains("aml_banks"));
    }

    @Test
    void test_structural_where_exists_subquery() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').where(outE('TRANSFER')).limit(10)",
                amlMapping()
        );

        String sql = result.sql();
        assertTrue(sql.contains("EXISTS"));
        assertTrue(sql.contains("aml_transfers"));
        assertTrue(sql.contains("LIMIT 10"));
    }

    @Test
    void test_structural_where_not_exists() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').where(not(outE('FLAGGED_BY'))).limit(10)",
                amlMapping()
        );

        String sql = result.sql();
        assertTrue(sql.contains("NOT"));
        assertTrue(sql.contains("EXISTS"));
        assertTrue(sql.contains("aml_account_alert"));
    }

    @Test
    void test_structural_multi_condition_where() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').where(and(outE('TRANSFER').has('isLaundering','1'), outE('FLAGGED_BY').count().is(0))).limit(10)",
                amlMapping()
        );

        String sql = result.sql();
        assertTrue(sql.contains("AND"));
        assertTrue(sql.contains("EXISTS"));
        assertTrue(sql.contains("COUNT(*)"));
        assertTrue(sql.contains("LIMIT 10"));
    }

    @Test
    void test_exact_suspicious_but_unflagged_projection() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').where(and(outE('TRANSFER').has('isLaundering','1'), outE('FLAGGED_BY').count().is(0))).project('accountId','bankId').by('accountId').by('bankId').limit(10)",
                amlMapping()
        );

        assertEquals(
                "SELECT v.account_id AS \"accountId\", v.bank_id AS \"bankId\" FROM aml_accounts v WHERE ((EXISTS (SELECT 1 FROM aml_transfers we WHERE we.from_account_id = v.id AND we.is_laundering = ?)) AND ((SELECT COUNT(*) FROM aml_account_alert we WHERE we.account_id = v.id) = ?)) LIMIT 10",
                result.sql()
        );
        assertEquals(List.of("1", "0"), result.parameters());
    }

    @Test
    void test_exact_suspicious_but_unflagged_with_degree_projection() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').where(and(outE('TRANSFER').has('isLaundering','1'), outE('FLAGGED_BY').count().is(0))).project('accountId','bankId','riskScore','suspiciousOut','totalOut').by('accountId').by('bankId').by('riskScore').by(outE('TRANSFER').has('isLaundering','1').count()).by(outE('TRANSFER').count()).order().by(select('suspiciousOut'), Order.desc).limit(20)",
                amlMapping()
        );

        assertEquals(
                "SELECT v.account_id AS \"accountId\", v.bank_id AS \"bankId\", v.risk_score AS \"riskScore\", (SELECT COUNT(*) FROM aml_transfers WHERE from_account_id = v.id AND is_laundering = '1') AS \"suspiciousOut\", (SELECT COUNT(*) FROM aml_transfers WHERE from_account_id = v.id) AS \"totalOut\" FROM aml_accounts v WHERE ((EXISTS (SELECT 1 FROM aml_transfers we WHERE we.from_account_id = v.id AND we.is_laundering = ?)) AND ((SELECT COUNT(*) FROM aml_account_alert we WHERE we.account_id = v.id) = ?)) ORDER BY \"suspiciousOut\" DESC LIMIT 20",
                result.sql()
        );
        assertEquals(List.of("1", "0"), result.parameters());
    }

    @Test
    void test_structural_path_with_join() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').limit(1).out('TRANSFER').path().by('accountId').limit(10)",
                amlMapping()
        );

        String sql = result.sql();
        assertTrue(sql.contains("JOIN"));
        assertTrue(sql.contains("account_id"));
    }

    @Test
    void test_structural_dedup_or_distinct() {
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').out('BELONGS_TO').dedup().limit(10)",
                amlMapping()
        );

        String sql = result.sql();
        assertTrue(sql.contains("DISTINCT") || sql.contains("GROUP BY"));
    }
}

