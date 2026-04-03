package com.graphqueryengine.query.translate.sql;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.translate.sql.dialect.IcebergSqlDialect;
import com.graphqueryengine.query.translate.sql.dialect.StandardSqlDialect;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that explicit outVertexLabel / inVertexLabel on EdgeMapping drives
 * target-vertex resolution deterministically, superseding column-stem and
 * edge-table-name heuristics.
 *
 * Also confirms backward compatibility: mappings without endpoint labels still
 * resolve correctly via existing heuristics.
 */
class EdgeMappingExplicitLabelTest {

    // ── Mapping helpers ───────────────────────────────────────────────────────

    /** Iceberg-style mapping WITH explicit endpoint labels. */
    private MappingConfig icebergWithLabels() {
        return new MappingConfig(
            Map.of(
                "Account", new VertexMapping("iceberg:aml.accounts", "id", Map.of(
                        "accountId", "account_id", "bankId", "bank_id", "riskScore", "risk_score")),
                "Bank",    new VertexMapping("iceberg:aml.banks",    "id", Map.of(
                        "bankId", "bank_id", "bankName", "bank_name", "countryCode", "country_code")),
                "Country", new VertexMapping("iceberg:aml.countries","id", Map.of(
                        "countryCode", "country_code", "countryName", "country_name",
                        "riskLevel", "risk_level", "fatfBlacklist", "fatf_blacklist", "region", "region")),
                "Alert",   new VertexMapping("iceberg:aml.alerts",   "id", Map.of(
                        "alertId", "alert_id", "severity", "severity",
                        "alertType", "alert_type", "status", "status", "raisedAt", "raised_at"))
            ),
            Map.of(
                // Generic in_id/out_id columns — heuristics would fail without explicit labels.
                "TRANSFER",  new EdgeMapping("iceberg:aml.transfers",     "id","out_id","in_id",
                        Map.of("isLaundering","is_laundering","amount","amount","currency","currency"),
                        "Account","Account"),
                "BELONGS_TO",new EdgeMapping("iceberg:aml.account_bank",  "id","out_id","in_id",
                        Map.of(), "Account","Bank"),
                "LOCATED_IN",new EdgeMapping("iceberg:aml.bank_country",  "id","out_id","in_id",
                        Map.of(), "Bank","Country"),
                "SENT_VIA",  new EdgeMapping("iceberg:aml.account_country","id","out_id","in_id",
                        Map.of("fatfBlacklist","fatf_blacklist"), "Account","Country"),
                "FLAGGED_BY",new EdgeMapping("iceberg:aml.account_alert", "id","out_id","in_id",
                        Map.of("severity","severity"), "Account","Alert")
            )
        );
    }

    /** Standard AML mapping WITHOUT explicit labels (backward-compat: heuristics). */
    private MappingConfig standardWithoutLabels() {
        return new MappingConfig(
            Map.of(
                "Account", new VertexMapping("aml_accounts","id", Map.of(
                        "accountId","account_id","bankId","bank_id","riskScore","risk_score")),
                "Bank",    new VertexMapping("aml_banks",   "id", Map.of(
                        "bankId","bank_id","bankName","bank_name","countryCode","country_code")),
                "Country", new VertexMapping("aml_countries","id", Map.of(
                        "countryCode","country_code","countryName","country_name",
                        "riskLevel","risk_level","fatfBlacklist","fatf_blacklist","region","region"))
            ),
            Map.of(
                "TRANSFER",  new EdgeMapping("aml_transfers",    "id","from_account_id","to_account_id",
                        Map.of("isLaundering","is_laundering","amount","amount","currency","currency")),
                "BELONGS_TO",new EdgeMapping("aml_account_bank", "id","account_id","bank_id", Map.of()),
                "LOCATED_IN",new EdgeMapping("aml_bank_country", "id","bank_id","country_id",  Map.of())
            )
        );
    }

    // ── Explicit-label tests ──────────────────────────────────────────────────

    @Test
    void explicitLabels_hopJoinsCorrectVertexTables() {
        // With generic out_id/in_id columns, only explicit labels can resolve
        // Bank and Country tables; heuristics would fail.
        var translator = new GremlinSqlTranslator(new IcebergSqlDialect());
        var r = translator.translate(
                "g.V().hasLabel('Account').limit(1)" +
                ".repeat(out('BELONGS_TO','LOCATED_IN').simplePath()).times(2)" +
                ".path().by('accountId').by('bankName').by('countryName').limit(10)",
                icebergWithLabels());
        String sql = r.sql();
        // Hop 1 must use aml.banks (Bank), not aml.accounts
        assertTrue(sql.contains("JOIN aml.banks v1"),    "hop-1 target must be Bank via explicit label");
        // Hop 2 must use aml.countries (Country), not aml.accounts
        assertTrue(sql.contains("JOIN aml.countries v2"),"hop-2 target must be Country via explicit label");
        assertFalse(sql.contains("LEFT JOIN"),           "no spurious LEFT JOINs");
        assertTrue(sql.endsWith("LIMIT 10"));
    }

    @Test
    void explicitLabels_neighborFoldSubqueryUsesCorrectTable() {
        var translator = new GremlinSqlTranslator(new IcebergSqlDialect());
        var r = translator.translate(
                "g.V().hasLabel('Account').limit(5)" +
                ".project('accountId','bankName')" +
                ".by('accountId').by(out('BELONGS_TO').values('bankName').fold())",
                icebergWithLabels());
        String sql = r.sql();
        // The correlated STRING_AGG subquery must join to aml.banks, not aml.accounts
        assertTrue(sql.contains("aml.banks"),            "fold subquery targets Bank vertex table");
        assertTrue(sql.contains("ARRAY_JOIN(ARRAY_AGG("),"Iceberg dialect uses ARRAY_JOIN");
        assertFalse(sql.contains("STRING_AGG"),          "no STRING_AGG in Iceberg dialect");
    }

    @Test
    void explicitLabels_whereOutNeighborHasUsesCorrectTable() {
        var translator = new GremlinSqlTranslator(new IcebergSqlDialect());
        var r = translator.translate(
                "g.V().hasLabel('Account')" +
                ".where(out('SENT_VIA').has('fatfBlacklist','true'))" +
                ".project('accountId','riskScore')" +
                ".by('accountId').by('riskScore').limit(10)",
                icebergWithLabels());
        String sql = r.sql();
        assertTrue(sql.contains("EXISTS"),                      "EXISTS subquery present");
        assertTrue(sql.contains("aml.account_country"),         "SENT_VIA edge table in EXISTS");
        // Country table drives the WHERE join for fatf_blacklist filter
        assertTrue(sql.contains("aml.countries"),               "Country vertex joined in EXISTS");
        assertTrue(sql.contains("fatf_blacklist"),               "filter column mapped");
        assertEquals(java.util.List.of("true"), r.parameters());
    }

    @Test
    void explicitLabels_whereOutEFlaggedByExistsUsesEdgeTable() {
        var translator = new GremlinSqlTranslator(new IcebergSqlDialect());
        var r = translator.translate(
                "g.V().hasLabel('Account').where(outE('FLAGGED_BY'))" +
                ".project('accountId').by('accountId').limit(5)",
                icebergWithLabels());
        String sql = r.sql();
        assertTrue(sql.contains("EXISTS"),             "EXISTS for where(outE)");
        assertTrue(sql.contains("aml.account_alert"),  "FLAGGED_BY Iceberg edge table");
        assertTrue(sql.endsWith("LIMIT 5"));
    }

    @Test
    void explicitLabels_edgeMappingFieldsRoundTrip() {
        // Verify the EdgeMapping record stores what we put in.
        EdgeMapping em = new EdgeMapping(
                "iceberg:aml.account_bank", "id", "out_id", "in_id",
                Map.of(), "Account", "Bank");
        assertEquals("Account", em.outVertexLabel());
        assertEquals("Bank",    em.inVertexLabel());
        assertEquals("aml.account_bank", em.table()); // iceberg: prefix stripped
    }

    @Test
    void explicitLabels_blankLabelsNormalizedToNull() {
        EdgeMapping em = new EdgeMapping(
                "aml_transfers", "id", "from_id", "to_id",
                Map.of(), "  ", "");
        assertNull(em.outVertexLabel(), "whitespace-only label normalized to null");
        assertNull(em.inVertexLabel(),  "empty label normalized to null");
    }

    @Test
    void explicitLabels_caseInsensitiveLookup() {
        // Upper-case label "ACCOUNT" should still find the "Account" vertex mapping.
        var translator = new GremlinSqlTranslator(new IcebergSqlDialect());
        MappingConfig m = new MappingConfig(
            Map.of("Account", new VertexMapping("iceberg:aml.accounts","id", Map.of("accountId","account_id"))),
            Map.of("TRANSFER", new EdgeMapping("iceberg:aml.transfers","id","out_id","in_id",
                    Map.of("isLaundering","is_laundering"), "ACCOUNT", "ACCOUNT"))
        );
        var r = translator.translate("g.V().hasLabel('Account').out('TRANSFER').limit(5)", m);
        String sql = r.sql();
        assertTrue(sql.contains("aml.accounts v1"), "case-insensitive label resolves Account table");
    }

    // ── Backward-compatibility tests (no explicit labels) ─────────────────────

    @Test
    void backwardCompat_noLabels_standardHeuristicsStillWork() {
        var translator = new GremlinSqlTranslator(new StandardSqlDialect());
        var r = translator.translate(
                "g.V().hasLabel('Account').limit(5)" +
                ".project('accountId','bankName')" +
                ".by('accountId').by(out('BELONGS_TO').values('bankName').fold())",
                standardWithoutLabels());
        String sql = r.sql();
        // Heuristics resolve bank_id → Bank → aml_banks
        assertTrue(sql.contains("aml_banks"),    "heuristic resolves Bank table without explicit labels");
        assertTrue(sql.contains("STRING_AGG"),   "standard dialect uses STRING_AGG");
        assertFalse(sql.contains("ARRAY_JOIN"), "no ARRAY_JOIN in standard dialect");
    }

    @Test
    void backwardCompat_noLabels_twoHopTraversalResolves() {
        var translator = new GremlinSqlTranslator(new StandardSqlDialect());
        var r = translator.translate(
                "g.V().hasLabel('Account').limit(1)" +
                ".out('BELONGS_TO').out('LOCATED_IN').limit(10)",
                standardWithoutLabels());
        String sql = r.sql();
        assertTrue(sql.contains("aml_account_bank"), "BELONGS_TO edge table in JOIN");
        assertTrue(sql.contains("aml_bank_country"), "LOCATED_IN edge table in JOIN");
        assertTrue(sql.endsWith("LIMIT 10"));
    }

    @Test
    void backwardCompat_fiveArgCtorProducesNullLabels() {
        EdgeMapping em = new EdgeMapping("t","id","out","in", Map.of());
        assertNull(em.outVertexLabel(), "legacy 5-arg ctor leaves outVertexLabel null");
        assertNull(em.inVertexLabel(),  "legacy 5-arg ctor leaves inVertexLabel null");
    }
}

