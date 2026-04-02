package com.graphqueryengine.query.translate.sql;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage tests for every Gremlin query used in the demo notebook
 * (aml_demo_queries.ipynb) — S1-S8 stats queries + C1-C11 analytics.
 * Mapping mirrors the full AML schema used in the notebook:
 *   Vertices : Account, Bank, Country, Alert
 *   Edges    : TRANSFER, BELONGS_TO, LOCATED_IN, SENT_VIA, FLAGGED_BY
 */
class NotebookQueryCoverageTest {

    private final GremlinSqlTranslator translator = new GremlinSqlTranslator();

    /** Full mapping matching the notebook schema (mirrors demo/mappings/aml-mapping.json). */
    private MappingConfig notebookMapping() {
        return new MappingConfig(
            Map.of(
                "Account", new VertexMapping("aml_accounts", "id", Map.of(
                        "accountId",   "account_id",
                        "bankId",      "bank_id",
                        "accountType", "account_type",
                        "riskScore",   "risk_score",
                        "isBlocked",   "is_blocked",
                        "openedDate",  "opened_date"
                )),
                "Bank", new VertexMapping("aml_banks", "id", Map.of(
                        "bankId",      "bank_id",
                        "bankName",    "bank_name",
                        "countryCode", "country_code",
                        "swiftCode",   "swift_code",
                        "tier",        "tier"
                )),
                "Country", new VertexMapping("aml_countries", "id", Map.of(
                        "countryCode",   "country_code",
                        "countryName",   "country_name",
                        "riskLevel",     "risk_level",
                        "fatfBlacklist", "fatf_blacklist",
                        "region",        "region"
                )),
                "Alert", new VertexMapping("aml_alerts", "id", Map.of(
                        "alertId",   "alert_id",
                        "alertType", "alert_type",
                        "status",    "status",
                        "severity",  "severity",
                        "raisedAt",  "raised_at"
                ))
            ),
            Map.of(
                "TRANSFER",  new EdgeMapping("aml_transfers",         "id", "from_account_id", "to_account_id", Map.of(
                        "isLaundering",  "is_laundering",
                        "amount",        "amount",
                        "currency",      "currency",
                        "paymentFormat", "payment_format",
                        "eventTime",     "event_time"
                )),
                "BELONGS_TO", new EdgeMapping("aml_account_bank",     "id", "account_id", "bank_id",     Map.of()),
                "LOCATED_IN", new EdgeMapping("aml_bank_country",     "id", "bank_id",    "country_id",  Map.of()),
                "SENT_VIA",   new EdgeMapping("aml_transfer_channel", "id", "transfer_id","country_id",  Map.of()),
                "FLAGGED_BY", new EdgeMapping("aml_account_alert",    "id", "account_id", "alert_id",    Map.of())
            )
        );
    }

    // ── S1-S5: Simple counts ──────────────────────────────────────────────────

    @Test
    void s1_countAccounts() {
        var r = translator.translate("g.V().hasLabel('Account').count()", notebookMapping());
        assertEquals("SELECT COUNT(*) AS count FROM aml_accounts", r.sql());
        assertTrue(r.parameters().isEmpty());
    }

    @Test
    void s2_countBanks() {
        var r = translator.translate("g.V().hasLabel('Bank').count()", notebookMapping());
        assertEquals("SELECT COUNT(*) AS count FROM aml_banks", r.sql());
    }

    @Test
    void s3_countCountries() {
        var r = translator.translate("g.V().hasLabel('Country').count()", notebookMapping());
        assertEquals("SELECT COUNT(*) AS count FROM aml_countries", r.sql());
    }

    @Test
    void s4_countAlerts() {
        var r = translator.translate("g.V().hasLabel('Alert').count()", notebookMapping());
        assertEquals("SELECT COUNT(*) AS count FROM aml_alerts", r.sql());
    }

    @Test
    void s5_countTransfers() {
        var r = translator.translate("g.E().hasLabel('TRANSFER').count()", notebookMapping());
        assertEquals("SELECT COUNT(*) AS count FROM aml_transfers", r.sql());
    }

    // ── S6: Suspicious transfer count (auto edge-label resolution) ────────────

    @Test
    void s6_suspiciousTransferCount() {
        var r = translator.translate("g.E().has('isLaundering','1').count()", notebookMapping());
        assertEquals("SELECT COUNT(*) AS count FROM aml_transfers WHERE is_laundering = ?", r.sql());
        assertEquals(List.of("1"), r.parameters());
    }

    // ── S7: High-risk country projection ─────────────────────────────────────

    @Test
    void s7_highRiskCountries() {
        var r = translator.translate(
                "g.V().hasLabel('Country').has('riskLevel','HIGH')" +
                ".project('countryCode','countryName','region','fatfBlacklist')" +
                ".by('countryCode').by('countryName').by('region').by('fatfBlacklist')",
                notebookMapping());
        String sql = r.sql();
        assertTrue(sql.contains("v.country_code AS \"countryCode\""), "countryCode projected");
        assertTrue(sql.contains("v.country_name AS \"countryName\""), "countryName projected");
        assertTrue(sql.contains("v.region AS \"region\""),            "region projected");
        assertTrue(sql.contains("v.fatf_blacklist AS \"fatfBlacklist\""), "fatfBlacklist projected");
        assertTrue(sql.contains("FROM aml_countries v"),             "base table");
        assertTrue(sql.contains("risk_level = ?"),                   "has() filter mapped");
        assertEquals(List.of("HIGH"), r.parameters());
    }

    // ── S8: High-severity alert projection ───────────────────────────────────

    @Test
    void s8_highSeverityAlerts() {
        var r = translator.translate(
                "g.V().hasLabel('Alert').has('severity','HIGH')" +
                ".project('alertId','alertType','status','raisedAt')" +
                ".by('alertId').by('alertType').by('status').by('raisedAt').limit(15)",
                notebookMapping());
        String sql = r.sql();
        assertTrue(sql.contains("v.alert_id AS \"alertId\""),     "alertId projected");
        assertTrue(sql.contains("v.alert_type AS \"alertType\""), "alertType projected");
        assertTrue(sql.contains("v.status AS \"status\""),        "status projected");
        assertTrue(sql.contains("v.raised_at AS \"raisedAt\""),   "raisedAt projected");
        assertTrue(sql.contains("FROM aml_alerts v"),             "base table");
        assertTrue(sql.contains("severity = ?"),                  "filter applied");
        assertTrue(sql.endsWith("LIMIT 15"),                      "limit applied");
        assertEquals(List.of("HIGH"), r.parameters());
    }

    // ── C1: Top sender accounts (outE count + order) ─────────────────────────

    @Test
    void c1_topSenderAccounts() {
        var r = translator.translate(
                "g.V().hasLabel('Account').project('accountId','bankId','outDegree')" +
                ".by('accountId').by('bankId').by(outE('TRANSFER').count())" +
                ".order().by(select('outDegree'),Order.desc).limit(15)",
                notebookMapping());
        String sql = r.sql();
        assertTrue(sql.contains("v.account_id AS \"accountId\""), "accountId projected");
        assertTrue(sql.contains("v.bank_id AS \"bankId\""),       "bankId projected");
        assertTrue(sql.contains("(SELECT COUNT(*) FROM aml_transfers WHERE from_account_id = v.id) AS \"outDegree\""),
                "outDegree correlated subquery");
        assertTrue(sql.contains("FROM aml_accounts v"),           "base table");
        assertTrue(sql.contains("ORDER BY \"outDegree\" DESC"),   "ordered desc");
        assertTrue(sql.endsWith("LIMIT 15"),                      "limit applied");
        assertTrue(r.parameters().isEmpty());
    }

    // ── C2: Suspicious hubs (filtered edge count + where(select.is(gt))) ─────

    @Test
    void c2_suspiciousHubs() {
        var r = translator.translate(
                "g.V().hasLabel('Account')" +
                ".project('accountId','bankId','suspiciousOut','totalOut')" +
                ".by('accountId').by('bankId')" +
                ".by(outE('TRANSFER').has('isLaundering','1').count())" +
                ".by(outE('TRANSFER').count())" +
                ".where(select('suspiciousOut').is(gt(0)))" +
                ".order().by(select('suspiciousOut'),Order.desc).limit(15)",
                notebookMapping());
        String sql = r.sql();
        assertTrue(sql.contains("(SELECT COUNT(*) FROM aml_transfers WHERE from_account_id = v.id AND is_laundering = '1') AS \"suspiciousOut\""),
                "filtered edge count");
        assertTrue(sql.contains("p.\"suspiciousOut\" > 0"), "where(gt(0)) filter");
        assertTrue(sql.contains("ORDER BY \"suspiciousOut\" DESC"), "ordered desc");
        assertTrue(sql.endsWith("LIMIT 15"), "limit");
        assertTrue(r.parameters().isEmpty());
    }

    // ── C3: Account → Bank via BELONGS_TO (fold projection) ──────────────────

    @Test
    void c3_accountBankBelongsTo() {
        var r = translator.translate(
                "g.V().hasLabel('Account').limit(15)" +
                ".project('accountId','bankId','bankName')" +
                ".by('accountId').by('bankId').by(out('BELONGS_TO').values('bankName').fold())",
                notebookMapping());
        String sql = r.sql();
        assertTrue(sql.contains("v.account_id AS \"accountId\""), "accountId");
        assertTrue(sql.contains("v.bank_id AS \"bankId\""),       "bankId");
        assertTrue(sql.contains("STRING_AGG"),                    "fold() → STRING_AGG");
        assertTrue(sql.contains("aml_account_bank"),              "BELONGS_TO edge");
        assertTrue(sql.contains("aml_banks"),                     "Bank vertex");
        assertTrue(sql.contains("bank_name"),                     "bank_name column");
        assertTrue(sql.contains("\"bankName\""),                  "bankName alias");
        assertFalse(sql.contains("LEFT JOIN aml_account_bank"),   "no outer LEFT JOIN");
        assertTrue(sql.endsWith("LIMIT 15"),                      "limit applied");
        assertTrue(r.parameters().isEmpty());
    }

    // ── C4: Bank → Country via LOCATED_IN (fold projection) ──────────────────

    @Test
    void c4_bankCountryLocatedIn() {
        var r = translator.translate(
                "g.V().hasLabel('Bank').limit(15)" +
                ".project('bankId','bankName','countryCode','countryName')" +
                ".by('bankId').by('bankName').by('countryCode').by(out('LOCATED_IN').values('countryName').fold())",
                notebookMapping());
        String sql = r.sql();
        assertTrue(sql.contains("v.bank_id AS \"bankId\""),           "bankId");
        assertTrue(sql.contains("v.bank_name AS \"bankName\""),       "bankName");
        assertTrue(sql.contains("v.country_code AS \"countryCode\""), "countryCode");
        assertTrue(sql.contains("STRING_AGG"),                        "fold() → STRING_AGG");
        assertTrue(sql.contains("aml_bank_country"),                  "LOCATED_IN edge");
        assertTrue(sql.contains("aml_countries"),                     "Country vertex");
        assertTrue(sql.contains("country_name"),                      "country_name column");
        assertFalse(sql.contains("LEFT JOIN aml_bank_country"),       "no outer LEFT JOIN");
        assertTrue(sql.endsWith("LIMIT 15"),                          "limit applied");
        assertTrue(r.parameters().isEmpty());
    }

    // ── C5: Two-hop Account→Bank→Country (multi-label repeat) ────────────────

    @Test
    void c5_twoHopAccountBankCountry() {
        var r = translator.translate(
                "g.V().hasLabel('Account').limit(1)" +
                ".repeat(out('BELONGS_TO','LOCATED_IN').simplePath()).times(2)" +
                ".path().by('accountId').by('bankName').by('countryName').limit(10)",
                notebookMapping());
        String sql = r.sql();
        assertTrue(sql.contains("aml_account_bank"),  "BELONGS_TO edge");
        assertTrue(sql.contains("aml_bank_country"),  "LOCATED_IN edge");
        assertTrue(sql.contains("LIMIT 1"),          "preHopLimit subquery");
        assertTrue(sql.contains("v1.id <> v0.id"),  "simplePath cycle check");
        assertTrue(sql.contains("account_id AS accountId0"), "path v0 column");
        assertTrue(sql.endsWith("LIMIT 10"),         "postHopLimit at end");
        assertTrue(r.parameters().isEmpty());
    }

    // ── C6: Accounts sending to high-risk countries (where out('SENT_VIA').has) ─

    @Test
    void c6_accountsSendingToHighRiskCountries() {
        var r = translator.translate(
                "g.V().hasLabel('Account')" +
                ".where(out('SENT_VIA').has('fatfBlacklist','true'))" +
                ".project('accountId','bankId','riskScore')" +
                ".by('accountId').by('bankId').by('riskScore').limit(20)",
                notebookMapping());
        String sql = r.sql();
        // Must have a vertex projection from aml_accounts
        assertTrue(sql.contains("v.account_id AS \"accountId\""), "accountId projected");
        assertTrue(sql.contains("v.bank_id AS \"bankId\""),       "bankId projected");
        assertTrue(sql.contains("v.risk_score AS \"riskScore\""), "riskScore projected");
        assertTrue(sql.contains("FROM aml_accounts v"),           "base table");
        // where(out('SENT_VIA').has('fatfBlacklist','true')) → EXISTS with join to Country
        assertTrue(sql.contains("EXISTS"),                    "EXISTS subquery for where(out...)");
        assertTrue(sql.contains("aml_transfer_channel"),      "SENT_VIA edge in EXISTS");
        assertTrue(sql.contains("fatf_blacklist"),            "fatfBlacklist column in EXISTS");
        assertTrue(sql.endsWith("LIMIT 20"),          "limit applied");
        assertEquals(List.of("true"), r.parameters());
    }

    // ── C7: Flagged accounts with alert detail (where outE + out count projection)

    @Test
    void c7_flaggedAccountsWithAlertDetail() {
        var r = translator.translate(
                "g.V().hasLabel('Account').where(outE('FLAGGED_BY'))" +
                ".project('accountId','bankId','alertCount','highSeverity')" +
                ".by('accountId').by('bankId')" +
                ".by(outE('FLAGGED_BY').count())" +
                ".by(out('FLAGGED_BY').has('severity','HIGH').count()).limit(20)",
                notebookMapping());
        String sql = r.sql();
        assertTrue(sql.contains("v.account_id AS \"accountId\""), "accountId projected");
        assertTrue(sql.contains("v.bank_id AS \"bankId\""),       "bankId projected");
        // alertCount: correlated outE count
        assertTrue(sql.contains("SELECT COUNT(*) FROM aml_account_alert WHERE account_id = v.id"),
                "alertCount correlated count on FLAGGED_BY edge");
        // highSeverity: filtered neighbor vertex count
        assertTrue(sql.contains("aml_alerts"),    "Alert vertex referenced");
        assertTrue(sql.contains("severity"),      "severity filter in subquery");
        // where(outE('FLAGGED_BY')) → EXISTS
        assertTrue(sql.contains("EXISTS"),         "EXISTS for where(outE(...))");
        assertTrue(sql.contains("aml_account_alert"), "FLAGGED_BY edge in EXISTS");
        assertTrue(sql.endsWith("LIMIT 20"),      "limit applied");
        assertTrue(r.parameters().isEmpty());
    }

    // ── C8: Cross-bank suspicious flow (edge projection with outV/inV) ───────

    @Test
    void c8_crossBankSuspiciousFlow() {
        var r = translator.translate(
                "g.E().has('isLaundering','1')" +
                ".project('fromBank','fromAcct','toBank','toAcct','amount','currency')" +
                ".by(outV().values('bankId')).by(outV().values('accountId'))" +
                ".by(inV().values('bankId')).by(inV().values('accountId'))" +
                ".by('amount').by('currency').limit(15)",
                notebookMapping());
        String sql = r.sql();
        assertTrue(sql.contains("ov.bank_id AS \"fromBank\""),    "fromBank");
        assertTrue(sql.contains("ov.account_id AS \"fromAcct\""), "fromAcct");
        assertTrue(sql.contains("iv.bank_id AS \"toBank\""),      "toBank");
        assertTrue(sql.contains("iv.account_id AS \"toAcct\""),   "toAcct");
        assertTrue(sql.contains("e.amount AS \"amount\""),        "amount");
        assertTrue(sql.contains("e.currency AS \"currency\""),    "currency");
        assertTrue(sql.contains("FROM aml_transfers e"),          "base edge table");
        assertTrue(sql.contains("JOIN aml_accounts ov"),          "outV join");
        assertTrue(sql.contains("JOIN aml_accounts iv"),          "inV join");
        assertTrue(sql.contains("WHERE e.is_laundering = ?"),     "has() filter");
        assertTrue(sql.endsWith("LIMIT 15"),                      "limit");
        assertEquals(List.of("1"), r.parameters());
    }

    // ── C9: Three-hop money trail ─────────────────────────────────────────────

    @Test
    void c9_threeHopMoneyTrail() {
        var r = translator.translate(
                "g.V().hasLabel('Account')" +
                ".where(outE('TRANSFER').has('isLaundering','1')).limit(1)" +
                ".repeat(out('TRANSFER').simplePath()).times(3)" +
                ".path().by('accountId').limit(10)",
                notebookMapping());
        String sql = r.sql();
        assertTrue(sql.contains("v0.account_id AS accountId0"), "path v0");
        assertTrue(sql.contains("v1.account_id AS accountId1"), "path v1");
        assertTrue(sql.contains("v2.account_id AS accountId2"), "path v2");
        assertTrue(sql.contains("v3.account_id AS accountId3"), "path v3");
        assertTrue(sql.contains("v1.id <> v0.id"),              "simplePath v1");
        assertTrue(sql.contains("v3.id NOT IN (v0.id, v1.id, v2.id)"), "simplePath v3");
        assertTrue(sql.contains("LIMIT 1"),                     "preHopLimit");
        assertTrue(sql.endsWith("LIMIT 10"),                    "postHopLimit");
        assertEquals(List.of("1"), r.parameters());
    }

    // ── C10: Five-hop layering chain ──────────────────────────────────────────

    @Test
    void c10_fiveHopLayeringChain() {
        var r = translator.translate(
                "g.V().hasLabel('Account')" +
                ".where(outE('TRANSFER').has('isLaundering','1')).limit(1)" +
                ".repeat(out('TRANSFER').simplePath()).times(5)" +
                ".path().by('accountId').limit(10)",
                notebookMapping());
        String sql = r.sql();
        assertTrue(sql.contains("v5.account_id AS accountId5"), "path v5");
        assertTrue(sql.contains("v5.id NOT IN (v0.id, v1.id, v2.id, v3.id, v4.id)"),
                "simplePath v5 cycle check");
        assertTrue(sql.contains("LIMIT 1"),   "preHopLimit");
        assertTrue(sql.endsWith("LIMIT 10"), "postHopLimit");
        assertEquals(List.of("1"), r.parameters());
    }

    // ── C11: Transactional suspicious count (same SQL as S6) ─────────────────

    @Test
    void c11_transactionalSuspiciousCount() {
        // Same query as S6 — the SQL translation should be identical
        var r = translator.translate("g.E().has('isLaundering','1').count()", notebookMapping());
        assertEquals("SELECT COUNT(*) AS count FROM aml_transfers WHERE is_laundering = ?", r.sql());
        assertEquals(List.of("1"), r.parameters());
    }
}


