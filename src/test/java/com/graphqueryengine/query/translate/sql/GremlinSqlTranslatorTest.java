package com.graphqueryengine.query.translate.sql;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.parser.AntlrGremlinTraversalParser;
import com.graphqueryengine.query.parser.model.GremlinParseResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GremlinSqlTranslatorTest {

    private final GremlinSqlTranslator translator = new GremlinSqlTranslator();

    @Test
    void translatesVertexQuery() {
        MappingConfig mappingConfig = sampleMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Person').has('name','Alice').values('age').limit(1)",
                mappingConfig
        );

        assertEquals("SELECT age AS age FROM people WHERE name = ? LIMIT 1", result.sql());
        assertEquals(1, result.parameters().size());
        assertEquals("Alice", result.parameters().get(0));
    }

    @Test
    void translatesEdgeQuery() {
        MappingConfig mappingConfig = sampleMapping();

        TranslationResult result = translator.translate(
                "g.E().hasLabel('KNOWS').has('since','2019')",
                mappingConfig
        );

        assertEquals("SELECT * FROM knows WHERE since_year = ?", result.sql());
        assertEquals("2019", result.parameters().get(0));
    }

    @Test
    void translatesVertexQueryAgainstIcebergTableReference() {
        MappingConfig mappingConfig = new MappingConfig(
                Map.of("Account", new VertexMapping("iceberg:s3://warehouse/aml/accounts", "id", Map.of(
                        "accountId", "account_id"
                ))),
                Map.of("TRANSFER", new EdgeMapping("aml_transfers", "id", "out_id", "in_id", Map.of()))
        );

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').values('accountId').limit(5)",
                mappingConfig
        );

        assertEquals(
                "SELECT account_id AS accountId FROM iceberg_scan('s3://warehouse/aml/accounts') LIMIT 5",
                result.sql()
        );
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void translatesVertexQueryAgainstIcebergCatalogIdentifier() {
        MappingConfig mappingConfig = new MappingConfig(
                Map.of("Account", new VertexMapping("iceberg:prod.aml.accounts", "id", Map.of(
                        "accountId", "account_id"
                ))),
                Map.of("TRANSFER", new EdgeMapping("aml_transfers", "id", "out_id", "in_id", Map.of()))
        );

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').values('accountId').limit(5)",
                mappingConfig
        );

        assertEquals(
                "SELECT account_id AS accountId FROM prod.aml.accounts LIMIT 5",
                result.sql()
        );
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void translatesTenHopOutTraversal() {
        MappingConfig mappingConfig = new MappingConfig(
                Map.of("Node", new VertexMapping("hop_nodes", "id", Map.of("name", "name"))),
                Map.of("LINK", new EdgeMapping("hop_links", "id", "out_id", "in_id", Map.of("weight", "weight")))
        );

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Node').has('id','1').out('LINK').out('LINK').out('LINK').out('LINK').out('LINK').out('LINK').out('LINK').out('LINK').out('LINK').out('LINK').values('name')",
                mappingConfig
        );

        assertTrue(result.sql().startsWith("SELECT v10.name AS name FROM hop_nodes v0 JOIN hop_links e1"));
        assertTrue(result.sql().contains("JOIN hop_links e10"));
        assertTrue(result.sql().contains("JOIN hop_nodes v10"));
        assertTrue(result.sql().contains("WHERE v0.id = ?"));
        assertEquals(1, result.parameters().size());
        assertEquals("1", result.parameters().get(0));
    }

    @Test
    void translatesRepeatTimesTenHopTraversal() {
        MappingConfig mappingConfig = new MappingConfig(
                Map.of("Node", new VertexMapping("hop_nodes", "id", Map.of("name", "name"))),
                Map.of("LINK", new EdgeMapping("hop_links", "id", "out_id", "in_id", Map.of("weight", "weight")))
        );

        TranslationResult result = translator.translate(
                "g.V(1).hasLabel('Node').repeat(out('LINK')).times(10).values('name')",
                mappingConfig
        );

        assertTrue(result.sql().startsWith("SELECT v10.name AS name FROM hop_nodes v0 JOIN hop_links e1"));
        assertTrue(result.sql().contains("JOIN hop_links e10"));
        assertTrue(result.sql().contains("JOIN hop_nodes v10"));
        assertTrue(result.sql().contains("WHERE v0.id = ?"));
        assertEquals(1, result.parameters().size());
        assertEquals("1", result.parameters().get(0));
    }

    @Test
    void translatesRepeatInTraversal() {
        MappingConfig mappingConfig = new MappingConfig(
                Map.of("Node", new VertexMapping("hop_nodes", "id", Map.of("name", "name"))),
                Map.of("LINK", new EdgeMapping("hop_links", "id", "out_id", "in_id", Map.of("weight", "weight")))
        );

        TranslationResult result = translator.translate(
                "g.V(11).hasLabel('Node').repeat(in('LINK')).times(10).values('name')",
                mappingConfig
        );

        assertTrue(result.sql().contains("ON e1.in_id = v0.id"));
        assertTrue(result.sql().contains("ON v1.id = e1.out_id"));
        assertTrue(result.sql().contains("JOIN hop_links e10"));
        assertEquals(1, result.parameters().size());
        assertEquals("11", result.parameters().get(0));
    }

    @Test
    void translatesRepeatBothTraversal() {
        MappingConfig mappingConfig = new MappingConfig(
                Map.of("Node", new VertexMapping("hop_nodes", "id", Map.of("name", "name"))),
                Map.of("LINK", new EdgeMapping("hop_links", "id", "out_id", "in_id", Map.of("weight", "weight")))
        );

        TranslationResult result = translator.translate(
                "g.V(1).hasLabel('Node').repeat(both('LINK')).times(1).values('name')",
                mappingConfig
        );

        assertTrue(result.sql().contains("UNION ALL"));
        assertTrue(result.sql().contains("ON e1.out_id = v0.id"));
        assertTrue(result.sql().contains("ON v1.id = e1.in_id"));
        assertTrue(result.sql().contains("ON e1.in_id = v0.id"));
        assertTrue(result.sql().contains("ON v1.id = e1.out_id"));
        assertEquals(2, result.parameters().size());
        assertEquals("1", result.parameters().get(0));
         assertEquals("1", result.parameters().get(1));
     }

     @Test
     void translatesEdgeGroupCountWithOrder() {
        MappingConfig mappingConfig = amlMapping();

        TranslationResult result = translator.translate(
                "g.E().groupCount().by('currency').order().by(select('currency'),Order.desc)",
                mappingConfig
        );

        assertEquals(
                "SELECT currency AS \"currency\", COUNT(*) AS count FROM aml_transfers GROUP BY currency ORDER BY \"currency\" DESC",
                result.sql()
        );
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void translatesVertexProjectWithOutEdgeCountAndOrder() {
        MappingConfig mappingConfig = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().project('accountId','bankId','outDegree').by('accountId').by('bankId').by(outE('TRANSFER').count()).order().by(select('outDegree'),Order.desc).limit(15)",
                mappingConfig
        );

        assertEquals(
                "SELECT v.account_id AS \"accountId\", v.bank_id AS \"bankId\", (SELECT COUNT(*) FROM aml_transfers WHERE out_id = v.id) AS \"outDegree\" FROM aml_accounts v ORDER BY \"outDegree\" DESC LIMIT 15",
                result.sql()
        );
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void translatesVertexProjectWithInEdgeCountAndOrder() {
        MappingConfig mappingConfig = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().project('accountId','inDegree').by('accountId').by(inE('TRANSFER').count()).order().by(select('inDegree'),Order.desc).limit(10)",
                mappingConfig
        );

        assertEquals(
                "SELECT v.account_id AS \"accountId\", (SELECT COUNT(*) FROM aml_transfers WHERE in_id = v.id) AS \"inDegree\" FROM aml_accounts v ORDER BY \"inDegree\" DESC LIMIT 10",
                result.sql()
        );
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void translatesVertexProjectWithChooseConstantProjection() {
        MappingConfig mapping = new MappingConfig(
                Map.of("Account", new VertexMapping("aml_accounts", "id", Map.of(
                        "accountId", "account_id",
                        "riskScore", "risk_score"
                ))),
                Map.of("TRANSFER", new EdgeMapping("aml_transfers", "id", "out_id", "in_id", Map.of()))
        );

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').limit(10).project('accountId','category')" +
                ".by('accountId')" +
                ".by(choose(values('riskScore').is(gt(0.7)), constant('WHALE'), constant('RETAIL')))",
                mapping
        );

        assertEquals(
                "SELECT v.account_id AS \"accountId\", CASE WHEN v.risk_score > 0.7 THEN 'WHALE' ELSE 'RETAIL' END AS \"category\" FROM aml_accounts v LIMIT 10",
                result.sql()
        );
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void translatesVertexProjectWithFilteredEdgeDegreeAndOrder() {
        MappingConfig mappingConfig = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().project('accountId','bankId','suspiciousOut','totalOut')" +
                ".by('accountId').by('bankId')" +
                ".by(outE('TRANSFER').has('isLaundering','1').count())" +
                ".by(outE('TRANSFER').count())" +
                ".order().by(select('suspiciousOut'),Order.desc).limit(15)",
                mappingConfig
        );

        assertEquals(
                "SELECT v.account_id AS \"accountId\", v.bank_id AS \"bankId\", " +
                "(SELECT COUNT(*) FROM aml_transfers WHERE out_id = v.id AND is_laundering = '1') AS \"suspiciousOut\", " +
                "(SELECT COUNT(*) FROM aml_transfers WHERE out_id = v.id) AS \"totalOut\" " +
                "FROM aml_accounts v ORDER BY \"suspiciousOut\" DESC LIMIT 15",
                result.sql()
        );
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void translatesEdgeToOutVertexTraversal() {
        MappingConfig mappingConfig = amlMapping();

        TranslationResult result = translator.translate(
                "g.E().has('isLaundering','1').outV().limit(20)",
                mappingConfig
        );

        assertEquals(
                "SELECT DISTINCT v.* FROM aml_accounts v JOIN aml_transfers e ON v.id = e.out_id WHERE e.is_laundering = ? LIMIT 20",
                result.sql()
        );
        assertEquals(List.of("1"), result.parameters());
    }

    @Test
    void translatesEdgeAliasSelectWithWhereNeq() {
        MappingConfig mappingConfig = amlMapping();

        TranslationResult result = translator.translate(
                "g.E().has('isLaundering','1').outV().as('a').outE('TRANSFER').has('isLaundering','1').inV().as('b').where('a',neq('b')).select('a','b').by('accountId').limit(20)",
                mappingConfig
        );

        assertEquals(
                "SELECT DISTINCT v1.account_id AS \"a\", v3.account_id AS \"b\" FROM aml_transfers e0 JOIN aml_accounts v1 ON v1.id = e0.out_id JOIN aml_transfers e2 ON e2.out_id = v1.id JOIN aml_accounts v3 ON v3.id = e2.in_id WHERE e0.is_laundering = ? AND e2.is_laundering = ? AND v1.id <> v3.id LIMIT 20",
                result.sql()
        );
        assertEquals(List.of("1", "1"), result.parameters());
    }

    @Test
    void translatesVertexProjectWithWhereSelectGt() {
        MappingConfig mappingConfig = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().project('accountId','bankId','suspiciousOut','totalOut').by('accountId').by('bankId').by(outE('TRANSFER').has('isLaundering','1').count()).by(outE('TRANSFER').count()).where(select('suspiciousOut').is(gt(0))).order().by(select('suspiciousOut'),Order.desc).limit(15)",
                mappingConfig
        );

        assertEquals(
                "SELECT * FROM (SELECT v.account_id AS \"accountId\", v.bank_id AS \"bankId\", (SELECT COUNT(*) FROM aml_transfers WHERE out_id = v.id AND is_laundering = '1') AS \"suspiciousOut\", (SELECT COUNT(*) FROM aml_transfers WHERE out_id = v.id) AS \"totalOut\" FROM aml_accounts v) p WHERE p.\"suspiciousOut\" > 0 ORDER BY \"suspiciousOut\" DESC LIMIT 15",
                result.sql()
        );
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void translatesWhereOutECountIsZeroBareLiteral() {
        MappingConfig mappingConfig = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').where(outE('TRANSFER').count().is(0)).project('accountId','bankId').by('accountId').by('bankId').limit(10)",
                mappingConfig
        );

        assertEquals(
                "SELECT v.account_id AS \"accountId\", v.bank_id AS \"bankId\" FROM aml_accounts v WHERE (SELECT COUNT(*) FROM aml_transfers we WHERE we.out_id = v.id) = ? LIMIT 10",
                result.sql()
        );
        assertEquals(List.of("0"), result.parameters());
    }

    @Test
    void translatesVertexWhereOutEHasWithProjectAfterBothAndDedup() {
        MappingConfig mappingConfig = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(1).as('hub').both('TRANSFER').dedup().project('accountId','bankId').by('accountId').by('bankId').limit(20)",
                mappingConfig
        );

        // both() generates a UNION of outgoing + incoming edges; dedup() → UNION (dedup between branches)
        assertTrue(result.sql().contains("UNION"), "both() traversal should produce a UNION");
        assertTrue(result.sql().contains("e1.out_id = v0.id"), "OUT branch should join on out_id");
        assertTrue(result.sql().contains("e1.in_id = v0.id"), "IN branch should join on in_id");
        assertEquals(List.of("1", "1"), result.parameters());
    }

    @Test
    void translatesRepeatBothSimplePathTraversal() {
        // Issue 2 applied to both(): simplePath() cycle detection must appear in BOTH UNION branches
        MappingConfig mappingConfig = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').has('accountId','ACC1').repeat(both('TRANSFER').simplePath()).times(2).values('accountId')",
                mappingConfig
        );

        // Both UNION branches must carry the same cycle-detection predicates
        String sql = result.sql();
        // OUT branch: v1 <> v0, v2 NOT IN (v0,v1)
        assertTrue(sql.contains("v1.id <> v0.id"), "OUT branch must prevent v1 = v0");
        assertTrue(sql.contains("v2.id NOT IN (v0.id, v1.id)"), "OUT branch must prevent v2 revisit");
        // IN branch (appears after UNION ALL): same predicates
        int firstIdx  = sql.indexOf("v1.id <> v0.id");
        int secondIdx = sql.lastIndexOf("v1.id <> v0.id");
        assertTrue(secondIdx > firstIdx, "cycle predicates must appear in both UNION branches");
        assertTrue(sql.contains("UNION ALL"), "no dedup → UNION ALL");
        assertEquals(2, result.parameters().size(), "one param per UNION branch for has() filter");
        assertEquals("ACC1", result.parameters().get(0));
        assertEquals("ACC1", result.parameters().get(1));
    }

    @Test
    void translatesBothRepeatPathBy() {
        // Issue 1 applied to both(): path().by('accountId') must select v0..vN from each UNION branch
        MappingConfig mappingConfig = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').has('accountId','ACC1').repeat(both('TRANSFER')).times(2).path().by('accountId').limit(10)",
                mappingConfig
        );

        String sql = result.sql();
        // Each UNION branch must select all hop vertex account_ids
        assertTrue(sql.contains("v0.account_id AS accountId0"), "path should include v0 in both branches");
        assertTrue(sql.contains("v1.account_id AS accountId1"), "path should include v1 in both branches");
        assertTrue(sql.contains("v2.account_id AS accountId2"), "path should include v2 in both branches");
        assertTrue(sql.contains("UNION ALL"), "no dedup → UNION ALL");
        assertTrue(sql.endsWith("LIMIT 10"), "postHopLimit at end");
        assertEquals(2, result.parameters().size());
    }

    @Test
    void translatesBothWithInlineFilterProducesCorrectParams() {
        // Issue 3 (param correctness): both() with an inline has() filter (no preHopLimit subquery)
        // must produce exactly 2 params — one for each UNION branch.
        MappingConfig mappingConfig = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').has('accountId','HUB1').both('TRANSFER').dedup().values('accountId')",
                mappingConfig
        );

        String sql = result.sql();
        assertTrue(sql.contains("UNION"), "both() must generate a UNION");
        assertTrue(sql.contains("e1.out_id = v0.id"), "OUT branch joins on out_id");
        assertTrue(sql.contains("e1.in_id = v0.id"),  "IN branch joins on in_id");
        // One param per UNION branch for has('accountId','HUB1')
        assertEquals(2, result.parameters().size(), "must have 1 param per UNION branch");
        assertEquals("HUB1", result.parameters().get(0));
        assertEquals("HUB1", result.parameters().get(1));
    }

    @Test
    void translatesBothWithInlineFilterAndWhereClauseProducesCorrectParams() {
        // Regression: both() with inline has() filter AND a where() clause must produce
        // independent, correctly ordered params for each UNION branch.
        MappingConfig mappingConfig = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').has('accountId','HUB1').where(outE('TRANSFER').has('isLaundering','1')).both('TRANSFER').dedup().values('accountId')",
                mappingConfig
        );

        String sql = result.sql();
        assertTrue(sql.contains("UNION"), "both() must generate a UNION");
        // Params: outBranch [HUB1, 1], inBranch [HUB1, 1] → total 4 in order
        assertEquals(4, result.parameters().size(), "2 params per UNION branch (filter + where)");
        assertEquals("HUB1", result.parameters().get(0));
        assertEquals("1",    result.parameters().get(1));
        assertEquals("HUB1", result.parameters().get(2));
        assertEquals("1",    result.parameters().get(3));
    }

    @Test
    void translatesVertexWhereOutEHasRepeatSimplePathAndPathBy() {
        MappingConfig mappingConfig = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(1).repeat(out('TRANSFER').simplePath()).times(3).path().by('accountId').limit(20)",
                mappingConfig
        );

        // path().by('accountId') — all hop vertices v0..v3 are selected
        assertTrue(result.sql().contains("v0.account_id AS accountId0"), "path should include v0");
        assertTrue(result.sql().contains("v1.account_id AS accountId1"), "path should include v1");
        assertTrue(result.sql().contains("v2.account_id AS accountId2"), "path should include v2");
        assertTrue(result.sql().contains("v3.account_id AS accountId3"), "path should include v3");
        // simplePath() — cycle detection predicates
        assertTrue(result.sql().contains("v1.id <> v0.id"), "simplePath should prevent v1 = v0");
        assertTrue(result.sql().contains("v2.id NOT IN (v0.id, v1.id)"), "simplePath should prevent v2 revisit");
        assertTrue(result.sql().contains("v3.id NOT IN (v0.id, v1.id, v2.id)"), "simplePath should prevent v3 revisit");
        // preHopLimit applied as start subquery, postHopLimit at end
        assertTrue(result.sql().contains("LIMIT 1"), "preHopLimit subquery");
        assertTrue(result.sql().endsWith("LIMIT 20"), "postHopLimit at end");
        assertEquals(List.of("1"), result.parameters());
    }

    @Test
    void translatesVertexWhereOutEHasRepeatDedupCount() {
        MappingConfig mappingConfig = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(1).repeat(out('TRANSFER')).times(2).dedup().count()",
                mappingConfig
        );

        assertEquals(
                "WITH _start AS (SELECT * FROM aml_accounts WHERE EXISTS (SELECT 1 FROM aml_transfers we WHERE we.out_id = id AND we.is_laundering = ?) LIMIT 1) SELECT COUNT(DISTINCT v2.id) AS count FROM _start v0 JOIN aml_transfers e1 ON e1.out_id = v0.id JOIN aml_accounts v1 ON v1.id = e1.in_id JOIN aml_transfers e2 ON e2.out_id = v1.id JOIN aml_accounts v2 ON v2.id = e2.in_id",
                result.sql()
        );
        assertEquals(List.of("1"), result.parameters());
    }

    @Test
    void translateString_matchesTranslateParsedModel() {
        MappingConfig mappingConfig = sampleMapping();
        String gremlin = "g.V(1).hasLabel('Person').has('name','Alice').values('age').limit(1)";

        TranslationResult fromString = translator.translate(gremlin, mappingConfig);
        GremlinParseResult parsed = new AntlrGremlinTraversalParser().parse(gremlin);
        TranslationResult fromParsed = translator.translate(parsed, mappingConfig);

        assertEquals(fromString.sql(), fromParsed.sql());
        assertEquals(fromString.parameters(), fromParsed.parameters());
    }

    private MappingConfig sampleMapping() {
        return new MappingConfig(
                Map.of("Person", new VertexMapping("people", "id", Map.of(
                        "name", "name",
                        "age", "age",
                        "city", "city"
                ))),
                Map.of("KNOWS", new EdgeMapping("knows", "id", "out_id", "in_id", Map.of(
                        "since", "since_year"
                )))
        );
    }

    private MappingConfig amlMapping() {
        return new MappingConfig(
                Map.of("Account", new VertexMapping("aml_accounts", "id", Map.of(
                        "accountId", "account_id",
                        "bankId", "bank_id"
                ))),
                Map.of("TRANSFER", new EdgeMapping("aml_transfers", "id", "out_id", "in_id", Map.of(
                        "currency", "currency",
                        "isLaundering", "is_laundering"
                )))
        );
    }

    /**
     * Full AML graph mapping with Account, Bank and Country vertices and
     * TRANSFER, BELONGS_TO and LOCATED_IN edges — used for multi-entity tests.
     */
    private MappingConfig fullAmlMapping() {
        return new MappingConfig(
                Map.of(
                        "Account", new VertexMapping("aml_accounts", "id", Map.of(
                                "accountId", "account_id",
                                "bankId",    "bank_id"
                        )),
                        "Bank", new VertexMapping("aml_banks", "id", Map.of(
                                "bankId",      "bank_id",
                                "bankName",    "bank_name",
                                "countryCode", "country_code"
                        )),
                        "Country", new VertexMapping("aml_countries", "id", Map.of(
                                "countryCode", "country_code",
                                "countryName", "country_name"
                        ))
                ),
                Map.of(
                        "TRANSFER", new EdgeMapping("aml_transfers", "id", "out_id", "in_id", Map.of(
                                "isLaundering", "is_laundering",
                                "amount",       "amount",
                                "currency",     "currency"
                        )),
                        "BELONGS_TO", new EdgeMapping("aml_belongs_to", "id", "account_id", "bank_id", Map.of()),
                        "LOCATED_IN", new EdgeMapping("aml_located_in", "id", "bank_id", "country_id", Map.of())
                )
        );
    }

    // -----------------------------------------------------------------------
    // Tests for newly supported features
    // -----------------------------------------------------------------------

    @Test
    void translatesEdgeCountWithAutoLabelResolutionFromHasFilter() {
        // g.E().has('isLaundering','1').count()
        // TRANSFER is the only edge mapping with 'isLaundering' — should auto-resolve
        MappingConfig mapping = fullAmlMapping();

        TranslationResult result = translator.translate(
                "g.E().has('isLaundering','1').count()",
                mapping
        );

        assertEquals("SELECT COUNT(*) AS count FROM aml_transfers WHERE is_laundering = ?", result.sql());
        assertEquals(List.of("1"), result.parameters());
    }

    @Test
    void translatesVertexProjectWithNeighborPropertyFold() {
        // g.V().hasLabel('Account').limit(15).project('accountId','bankId','bankName')
        //       .by('accountId').by('bankId').by(out('BELONGS_TO').values('bankName').fold())
        // fold() maps to STRING_AGG subquery — one row per account, no duplicate fan-out.
        MappingConfig mapping = fullAmlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').limit(15).project('accountId','bankId','bankName')" +
                ".by('accountId').by('bankId').by(out('BELONGS_TO').values('bankName').fold())",
                mapping
        );

        String sql = result.sql();
        // Must project the three fields
        assertTrue(sql.contains("v.account_id AS \"accountId\""), "accountId projected from Account");
        assertTrue(sql.contains("v.bank_id AS \"bankId\""),       "bankId projected from Account");
        // bankName is via STRING_AGG correlated subquery — no LEFT JOIN on the outer query
        assertTrue(sql.contains("STRING_AGG"),                    "fold() uses STRING_AGG aggregate");
        assertTrue(sql.contains("aml_belongs_to"),                "BELONGS_TO edge referenced in subquery");
        assertTrue(sql.contains("aml_banks"),                     "Bank vertex referenced in subquery");
        assertTrue(sql.contains("bank_name"),                     "bank_name column selected");
        assertTrue(sql.contains("\"bankName\""),                  "alias bankName present");
        assertTrue(sql.contains("FROM aml_accounts v"),           "base table is Account");
        // Must NOT have an outer LEFT JOIN (avoids duplicate rows per account)
        assertFalse(sql.contains("LEFT JOIN aml_belongs_to"),     "no outer LEFT JOIN for fold()");
        assertEquals(0, result.parameters().size());
    }

    @Test
    void translatesNeighborPropertyFoldWithGenericEdgeColumns() {
        MappingConfig mapping = new MappingConfig(
                Map.of(
                        "Account", new VertexMapping("aml.accounts", "id", Map.of(
                                "accountId", "account_id",
                                "bankId", "bank_id"
                        )),
                        "Bank", new VertexMapping("aml.banks", "id", Map.of(
                                "bankId", "bank_id",
                                "bankName", "bank_name"
                        )),
                        "Country", new VertexMapping("aml.countries", "id", Map.of(
                                "countryName", "country_name"
                        ))
                ),
                Map.of(
                        "BELONGS_TO", new EdgeMapping("aml.account_bank", "id", "out_id", "in_id", Map.of()),
                        "LOCATED_IN", new EdgeMapping("aml.bank_country", "id", "out_id", "in_id", Map.of())
                )
        );

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').limit(15).project('accountId','bankId','bankName')"
                        + ".by('accountId').by('bankId').by(out('BELONGS_TO').values('bankName').fold())",
                mapping
        );

        String sql = result.sql();
        assertTrue(sql.contains("FROM aml.accounts v"), "base table should be Account");
        assertTrue(sql.contains("aml.account_bank"), "BELONGS_TO edge table should be referenced");
        assertTrue(sql.contains("aml.banks"), "neighbor table should resolve to Bank");
        assertTrue(sql.contains("bank_name"), "neighbor property should resolve to bank_name");
    }

    @Test
    void translatesVertexProjectWithNeighborPropertyFoldChain() {
        // g.V().hasLabel('Bank').limit(15).project('bankId','bankName','countryCode','countryName')
        //       .by('bankId').by('bankName').by('countryCode').by(out('LOCATED_IN').values('countryName').fold())
        MappingConfig mapping = fullAmlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Bank').limit(15).project('bankId','bankName','countryCode','countryName')" +
                ".by('bankId').by('bankName').by('countryCode').by(out('LOCATED_IN').values('countryName').fold())",
                mapping
        );

        String sql = result.sql();
        assertTrue(sql.contains("v.bank_id AS \"bankId\""),          "bankId from Bank");
        assertTrue(sql.contains("v.bank_name AS \"bankName\""),      "bankName from Bank");
        assertTrue(sql.contains("v.country_code AS \"countryCode\""),"countryCode from Bank");
        // countryName via STRING_AGG subquery
        assertTrue(sql.contains("STRING_AGG"),                       "fold() uses STRING_AGG aggregate");
        assertTrue(sql.contains("aml_located_in"),                   "LOCATED_IN edge in subquery");
        assertTrue(sql.contains("aml_countries"),                    "Country vertex in subquery");
        assertTrue(sql.contains("country_name"),                     "country_name column selected");
        assertTrue(sql.contains("\"countryName\""),                  "alias countryName present");
        assertTrue(sql.contains("FROM aml_banks v"),                 "base table is Bank");
        assertFalse(sql.contains("LEFT JOIN aml_located_in"),        "no outer LEFT JOIN for fold()");
        assertEquals(0, result.parameters().size());
    }

    @Test
    void translatesRepeatWithMultipleEdgeLabels() {
        // g.V().hasLabel('Account').limit(1)
        //       .repeat(out('BELONGS_TO','LOCATED_IN').simplePath()).times(2)
        //       .path().by('accountId').by('bankName').by('countryName').limit(10)
        //
        // Multi-hop multi-label uses a schema-aware linear LEFT JOIN chain:
        //   hop1 primary = BELONGS_TO (Account→Bank), plus LEFT JOIN LOCATED_IN
        //   hop2 primary = BELONGS_TO again (Bank→Bank), plus LEFT JOIN LOCATED_IN
        // This avoids the O(labels^hops) UNION explosion.
        MappingConfig mapping = fullAmlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').limit(1)" +
                ".repeat(out('BELONGS_TO','LOCATED_IN').simplePath()).times(2)" +
                ".path().by('accountId').by('bankName').by('countryName').limit(10)",
                mapping
        );

        String sql = result.sql();
        // Both edge tables must appear (one primary JOIN + one LEFT JOIN per hop)
        assertTrue(sql.contains("aml_belongs_to"), "BELONGS_TO edge must appear");
        assertTrue(sql.contains("aml_located_in"), "LOCATED_IN edge must appear");
        // v0 must be wrapped in a subquery with LIMIT 1 (preHopLimit)
        assertTrue(sql.contains("SELECT * FROM aml_accounts") && sql.contains("LIMIT 1"),
                "v0 subquery must carry LIMIT 1");
        // simplePath cycle predicates
        assertTrue(sql.contains("v1.id <> v0.id"), "simplePath v1 != v0");
        // Path columns from all three vertex positions
        assertTrue(sql.contains("account_id AS accountId0"), "path: v0 accountId");
        // Post-hop limit at the end
        assertTrue(sql.endsWith("LIMIT 10"), "post-hop LIMIT 10 at end");
        // hasLabel() resolves the vertex table but does not add a bind parameter
        assertTrue(result.parameters().isEmpty(), "no bind params (hasLabel is table-resolved, not parameterised)");
    }

    @Test
    void translatesEdgeProjectWithMultiVertexMappings() {
        // g.E().has('isLaundering','1').project('fromBank','fromAcct','toBank','toAcct','amount','currency')
        //       .by(outV().values('bankId')).by(outV().values('accountId'))
        //       .by(inV().values('bankId')).by(inV().values('accountId'))
        //       .by('amount').by('currency').limit(15)
        MappingConfig mapping = fullAmlMapping();

        TranslationResult result = translator.translate(
                "g.E().has('isLaundering','1')" +
                ".project('fromBank','fromAcct','toBank','toAcct','amount','currency')" +
                ".by(outV().values('bankId')).by(outV().values('accountId'))" +
                ".by(inV().values('bankId')).by(inV().values('accountId'))" +
                ".by('amount').by('currency').limit(15)",
                mapping
        );

        String sql = result.sql();
        // Edge TRANSFER: out_id → Account (outV), in_id → Account (inV)
        assertTrue(sql.contains("ov.bank_id AS \"fromBank\""),    "outV bankId");
        assertTrue(sql.contains("ov.account_id AS \"fromAcct\""), "outV accountId");
        assertTrue(sql.contains("iv.bank_id AS \"toBank\""),      "inV bankId");
        assertTrue(sql.contains("iv.account_id AS \"toAcct\""),   "inV accountId");
        assertTrue(sql.contains("e.amount AS \"amount\""),        "edge amount");
        assertTrue(sql.contains("e.currency AS \"currency\""),    "edge currency");
        assertTrue(sql.contains("FROM aml_transfers e"),          "base edge table");
        assertTrue(sql.contains("JOIN aml_accounts ov"),          "outV join");
        assertTrue(sql.contains("JOIN aml_accounts iv"),          "inV join");
        assertTrue(sql.contains("WHERE e.is_laundering = ?"),     "has() filter");
        assertTrue(sql.endsWith("LIMIT 15"),                      "limit");
        assertEquals(List.of("1"), result.parameters());
    }

    @Test
    void translatesOutWithMultipleLabelsProducesUnion() {
        // Single hop with two labels → two UNION branches
        MappingConfig mapping = fullAmlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').has('accountId','A1').out('BELONGS_TO','TRANSFER').dedup().values('bankId')",
                mapping
        );

        String sql = result.sql();
        assertTrue(sql.contains("UNION"), "multi-label out() must produce UNION");
        assertTrue(sql.contains("aml_belongs_to"), "BELONGS_TO branch");
        assertTrue(sql.contains("aml_transfers"),  "TRANSFER branch");
        // Both branches should filter on v0.account_id
        assertEquals(2, result.parameters().size(), "one param per UNION branch");
        assertEquals("A1", result.parameters().get(0));
        assertEquals("A1", result.parameters().get(1));
    }

    @Test
    void translatesBothWithPreHopLimitAndNoWhereUsesSubquery() {
        // Regression: both() with only a preHopLimit (no where/filter) must still wrap v0 in a
        // subquery. The old bug condition `preHopLimit!=null && (whereClause!=null||!filters.isEmpty())`
        // would have skipped the subquery, producing an invalid bare LIMIT in the FROM clause.
        MappingConfig mapping = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').limit(1).both('TRANSFER').dedup().values('accountId')",
                mapping
        );

        String sql = result.sql();
        assertTrue(sql.contains("UNION"), "both() must produce a UNION");
        // v0 must be wrapped: the LIMIT 1 should appear inside a CTE or subquery, not as a bare LIMIT
        assertTrue(
                sql.contains("(SELECT * FROM aml_accounts LIMIT 1) v0")
                || sql.contains("WITH _start AS (SELECT * FROM aml_accounts LIMIT 1)"),
                "preHopLimit must be inside a CTE or subquery even without where/filter");
        assertTrue(result.parameters().isEmpty(), "no params (hasLabel resolves table only)");
    }

    @Test
    void translatesEdgeAsAliasBeforeProjectIsIgnored() {
        // g.E().has('isLaundering','1').as('e').project(...).by(outV()...) — the .as('e') on
        // the edge is parsed but does not affect the projection SQL (no .select() step).
        // Use fullAmlMapping() so 'amount' is available on TRANSFER.
        MappingConfig mapping = fullAmlMapping();

        TranslationResult result = translator.translate(
                "g.E().has('isLaundering','1').as('e')" +
                ".project('fromAcct','toAcct','amount')" +
                ".by(outV().values('accountId')).by(inV().values('accountId')).by('amount')" +
                ".limit(15)",
                mapping
        );

        String sql = result.sql();
        // Must produce the same SQL as without .as('e')
        assertTrue(sql.contains("ov.account_id AS \"fromAcct\""), "outV accountId");
        assertTrue(sql.contains("iv.account_id AS \"toAcct\""),   "inV accountId");
        assertTrue(sql.contains("e.amount AS \"amount\""),        "edge amount");
        assertTrue(sql.contains("e.is_laundering = ?"),           "has() filter");
        assertTrue(sql.endsWith("LIMIT 15"),                      "limit");
        assertEquals(List.of("1"), result.parameters());
    }

    @Test
    void translatesEdgeNeqSelectChainWithAutoLabelResolution() {
        // Shell script: g.E().has('isLaundering','1').outV().as('a').outE('TRANSFER').has('isLaundering','1')
        //               .inV().as('b').where('a',neq('b')).select('a','b').by('accountId').limit(20)
        // The auto-label resolution must pick TRANSFER for the root g.E() (only edge with isLaundering).
        MappingConfig mapping = amlMapping();

        TranslationResult result = translator.translate(
                "g.E().has('isLaundering','1').outV().as('a')" +
                ".outE('TRANSFER').has('isLaundering','1').inV().as('b')" +
                ".where('a',neq('b')).select('a','b').by('accountId').limit(20)",
                mapping
        );

        String sql = result.sql();
        assertTrue(sql.contains("v1.account_id AS \"a\""), "alias a = accountId");
        assertTrue(sql.contains("v3.account_id AS \"b\""), "alias b = accountId");
        assertTrue(sql.contains("v1.id <> v3.id"),         "neq where clause");
        assertTrue(sql.contains("e0.is_laundering = ?"),   "root edge filter");
        assertTrue(sql.contains("e2.is_laundering = ?"),   "hop edge filter");
        assertTrue(sql.endsWith("LIMIT 20"),               "limit");
        assertEquals(List.of("1", "1"), result.parameters());
    }

    @Test
    void translatesHubBothDedupProjectWithPreHopLimitAndWhere() {
        // Shell script: g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(1).as('hub')
        //               .both('TRANSFER').dedup().project('accountId','bankId').by('accountId').by('bankId').limit(20)
        // Tests: where(outE) + preHopLimit + as() + both() + dedup() + project()
        MappingConfig mapping = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().where(outE('TRANSFER').has('isLaundering','1')).limit(1).as('hub')" +
                ".both('TRANSFER').dedup().project('accountId','bankId')" +
                ".by('accountId').by('bankId').limit(20)",
                mapping
        );

        String sql = result.sql();
        assertTrue(sql.contains("UNION"), "both() generates UNION");
        // v0 subquery must contain the where clause and LIMIT 1
        assertTrue(sql.contains("EXISTS"), "where(outE) → EXISTS in subquery");
        assertTrue(sql.contains("LIMIT 1"), "preHopLimit inside subquery");
        // Project columns from the final vertex alias
        assertTrue(sql.contains("account_id AS \"accountId\""), "accountId projected");
        assertTrue(sql.contains("bank_id AS \"bankId\""),       "bankId projected");
        // dedup → UNION (not UNION ALL)
        assertTrue(sql.contains("UNION") && !sql.contains("UNION ALL"), "dedup → UNION");
        assertTrue(sql.endsWith("LIMIT 20"), "postHopLimit at end");
        // The WHERE subquery (containing the EXISTS bind param '1') is inlined in both UNION
        // branches, so the param appears once per branch → [1, 1].
        assertEquals(List.of("1", "1"), result.parameters());
    }

    @Test
    void translatesTerminalOutEPathWithEdgePropertyFilter() {
        MappingConfig mapping = new MappingConfig(
                Map.of("Account", new VertexMapping("aml_accounts", "id", Map.of(
                        "accountId", "account_id",
                        "riskScore", "risk_score"
                ))),
                Map.of("TRANSFER", new EdgeMapping("aml_transfers", "id", "out_id", "in_id", Map.of(
                        "amount", "amount"
                )))
        );

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').has('riskScore', gt(0.8)).outE('TRANSFER').has('amount', gt(1000000)).path().by('accountId').by('amount').limit(10)",
                mapping
        );

        assertEquals(
                "SELECT v0.account_id AS accountId0, e1.amount AS amount1 FROM aml_accounts v0 JOIN aml_transfers e1 ON e1.out_id = v0.id WHERE v0.risk_score > ? AND e1.amount > ? LIMIT 10",
                result.sql()
        );
        assertEquals(List.of(0.8, 1000000L), result.parameters());
    }

    @Test
    void translatesAliasNeqByPropertyAcrossHopAliases() {
        MappingConfig mapping = new MappingConfig(
                Map.of("Account", new VertexMapping("aml_accounts", "id", Map.of(
                        "accountId", "account_id",
                        "bankId", "bank_id"
                ))),
                Map.of("TRANSFER", new EdgeMapping("aml_transfers", "id", "from_account_id", "to_account_id", Map.of()))
        );

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').as('a').out('TRANSFER').as('b').where('a',neq('b')).by('bankId').path().by('accountId').limit(10)",
                mapping
        );

        assertEquals(
                "SELECT v0.account_id AS accountId0, v1.account_id AS accountId1 FROM aml_accounts v0 JOIN aml_transfers e1 ON e1.from_account_id = v0.id JOIN aml_accounts v1 ON v1.id = e1.to_account_id WHERE v0.bank_id <> v1.bank_id LIMIT 10",
                result.sql()
        );
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void translatesTerminalOutEWithAmlDirectionColumns() {
        MappingConfig mapping = new MappingConfig(
                Map.of("Account", new VertexMapping("aml_accounts", "id", Map.of(
                        "accountId", "account_id"
                ))),
                Map.of("TRANSFER", new EdgeMapping("aml_transfers", "id", "from_account_id", "to_account_id", Map.of(
                        "amount", "amount"
                )))
        );

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').outE('TRANSFER').has('amount', gt(100)).path().by('accountId').by('amount').limit(5)",
                mapping
        );

        assertEquals(
                "SELECT v0.account_id AS accountId0, e1.amount AS amount1 FROM aml_accounts v0 JOIN aml_transfers e1 ON e1.from_account_id = v0.id WHERE e1.amount > ? LIMIT 5",
                result.sql()
        );
        assertEquals(List.of(100L), result.parameters());
    }

    @Test
    void translatesAliasKeyedGroupCountWithOutEUsesOutColumnDirection() {
        MappingConfig mapping = new MappingConfig(
                Map.of("Account", new VertexMapping("aml_accounts", "id", Map.of(
                        "accountId", "account_id"
                ))),
                Map.of("TRANSFER", new EdgeMapping("aml_transfers", "id", "from_account_id", "to_account_id", Map.of(
                        "amount", "amount"
                )))
        );

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').as('a').outE('TRANSFER').groupCount().by(select('a').by('accountId')).order(local).by(values, desc).limit(local, 15)",
                mapping
        );

        assertEquals(
                "SELECT v0.account_id AS \"accountId\", COUNT(*) AS count FROM aml_accounts v0 JOIN aml_transfers e1 ON e1.from_account_id = v0.id JOIN aml_accounts v1 ON v1.id = e1.to_account_id GROUP BY v0.account_id ORDER BY count DESC LIMIT 15",
                result.sql()
        );
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void translatesHasIdOnVertexRoot() {
        MappingConfig mapping = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').hasId('A-1').values('accountId')",
                mapping
        );

        assertEquals("SELECT account_id AS accountId FROM aml_accounts WHERE id = ?", result.sql());
        assertEquals(List.of("A-1"), result.parameters());
    }

    @Test
    void translatesHasNotAsIsNullPredicate() {
        MappingConfig mapping = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').hasNot('bankId').count()",
                mapping
        );

        assertEquals("SELECT COUNT(*) AS count FROM aml_accounts WHERE bank_id IS NULL", result.sql());
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void translatesValueMapOnVertexRoot() {
        MappingConfig mapping = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').valueMap().limit(2)",
                mapping
        );

        assertTrue(result.sql().startsWith("SELECT "));
        assertTrue(result.sql().contains("account_id AS \"accountId\""));
        assertTrue(result.sql().contains("bank_id AS \"bankId\""));
        assertTrue(result.sql().contains(" FROM aml_accounts LIMIT 2"));
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void translatesEdgeBothVToUnionOfEndpoints() {
        MappingConfig mapping = amlMapping();

        TranslationResult result = translator.translate(
                "g.E().hasLabel('TRANSFER').has('isLaundering','1').bothV().limit(5)",
                mapping
        );

        assertTrue(result.sql().contains("UNION ALL"));
        assertTrue(result.sql().contains("e.out_id"));
        assertTrue(result.sql().contains("e.in_id"));
        assertEquals(List.of("1", "1"), result.parameters());
    }

    @Test
    void translatesProjectByIdentityOnVertices() {
        MappingConfig mapping = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').project('id','accountId').by(identity()).by('accountId').limit(3)",
                mapping
        );

        assertEquals(
                "SELECT v.id AS \"id\", v.account_id AS \"accountId\" FROM aml_accounts v LIMIT 3",
                result.sql()
        );
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void translatesValuesWithIsPredicate() {
        MappingConfig mapping = sampleMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Person').values('age').is(gt(30)).limit(2)",
                mapping
        );

        assertEquals("SELECT age AS age FROM people WHERE age > ? LIMIT 2", result.sql());
        assertEquals(List.of(30L), result.parameters());
    }

    @Test
    void translatesWhereAndPredicates() {
        MappingConfig mapping = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').where(and(outE('TRANSFER').has('isLaundering','1'),inE('TRANSFER').has('currency','USD'))).count()",
                mapping
        );

        assertTrue(result.sql().contains("WHERE ("));
        assertTrue(result.sql().contains("EXISTS (SELECT 1 FROM aml_transfers we WHERE we.out_id = v.id AND we.is_laundering = ?)"));
        assertTrue(result.sql().contains("EXISTS (SELECT 1 FROM aml_transfers we WHERE we.in_id = v.id AND we.currency = ?)"));
        assertEquals(List.of("1", "USD"), result.parameters());
    }

    @Test
    void translatesWhereOrPredicates() {
        MappingConfig mapping = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').where(or(outE('TRANSFER').has('isLaundering','1'),inE('TRANSFER').has('currency','USD'))).count()",
                mapping
        );

        assertTrue(result.sql().contains(" OR "));
        assertTrue(result.sql().contains("we.is_laundering = ?"));
        assertTrue(result.sql().contains("we.currency = ?"));
        assertEquals(List.of("1", "USD"), result.parameters());
    }

    @Test
    void translatesWhereNotPredicate() {
        MappingConfig mapping = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').where(not(outE('TRANSFER').has('isLaundering','1'))).count()",
                mapping
        );

        assertTrue(result.sql().contains("WHERE NOT (EXISTS (SELECT 1 FROM aml_transfers we WHERE we.out_id = v.id AND we.is_laundering = ?))"));
        assertEquals(List.of("1"), result.parameters());
    }

    @Test
    void translatesBothEAsHopStep() {
        MappingConfig mapping = amlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').bothE('TRANSFER').has('isLaundering','1').values('currency').limit(5)",
                mapping
        );

        assertTrue(result.sql().contains("SELECT e1.currency AS currency"));
        assertTrue(result.sql().contains("ON (e1.out_id = v0.id OR e1.in_id = v0.id)"));
        assertTrue(result.sql().contains("WHERE e1.is_laundering = ?"));
        assertTrue(result.sql().endsWith("LIMIT 5"));
        assertEquals(List.of("1"), result.parameters());
    }

    @Test
    void translatesOrderByPropertyWithOrderDescToken() {
        MappingConfig mapping = new MappingConfig(
                Map.of("Account", new VertexMapping("aml_accounts", "id", Map.of(
                        "riskScore", "risk_score"
                ))),
                Map.of("TRANSFER", new EdgeMapping("aml_transfers", "id", "out_id", "in_id", Map.of()))
        );

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').order().by('riskScore', Order.desc).values('riskScore').limit(10)",
                mapping
        );

        assertEquals(
                "SELECT risk_score AS riskScore FROM aml_accounts ORDER BY \"riskScore\" DESC LIMIT 10",
                result.sql()
        );
        assertTrue(result.parameters().isEmpty());
    }

    @Test
    void translatesHopProjectionUsingTerminalVertexMapping() {
        MappingConfig mapping = fullAmlMapping();

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').limit(5).out('BELONGS_TO').project('bankId','bankName').by('bankId').by('bankName')",
                mapping
        );

        assertTrue(result.sql().contains("bank_id AS \"bankId\""));
        assertTrue(result.sql().contains("bank_name AS \"bankName\""));
        assertTrue(result.sql().contains("JOIN aml_belongs_to e1"));
        assertTrue(result.sql().contains("JOIN aml_banks v1"));
        assertTrue(result.sql().contains("LIMIT 5"));
    }

    @Test
    void translatesEdgeOutVProjectionUsingEndpointVertexProperties() {
        MappingConfig mapping = new MappingConfig(
                Map.of(
                        "Account", new VertexMapping("aml_accounts", "id", Map.of(
                                "accountId", "account_id",
                                "bankId", "bank_id"
                        ))
                ),
                Map.of(
                        "TRANSFER", new EdgeMapping("aml_transfers", "id", "out_id", "in_id", Map.of(), "Account", "Account")
                )
        );

        TranslationResult result = translator.translate(
                "g.E().hasLabel('TRANSFER').limit(5).outV().project('accountId','bankId').by('accountId').by('bankId')",
                mapping
        );

        assertTrue(result.sql().contains("SELECT DISTINCT v.account_id AS \"accountId\", v.bank_id AS \"bankId\""));
        assertTrue(result.sql().contains("FROM aml_accounts v JOIN aml_transfers e ON v.id = e.out_id"));
        assertTrue(result.sql().endsWith("LIMIT 5"));
    }

    @Test
    void translatesMeanAfterProjectSelectAliasOnTerminalOutE() {
        MappingConfig mapping = new MappingConfig(
                Map.of(
                        "Country", new VertexMapping("aml_countries", "id", Map.of("countryName", "country_name")),
                        "Bank", new VertexMapping("aml_banks", "id", Map.of()),
                        "Account", new VertexMapping("aml_accounts", "id", Map.of())
                ),
                Map.of(
                        "LOCATED_IN", new EdgeMapping("aml_bank_country", "id", "bank_id", "country_id", Map.of()),
                        "BELONGS_TO", new EdgeMapping("aml_account_bank", "id", "account_id", "bank_id", Map.of()),
                        "TRANSFER", new EdgeMapping("aml_transfers", "id", "from_account_id", "to_account_id", Map.of("amount", "amount"))
                )
        );

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Country').has('countryName', 'Singapore').in('LOCATED_IN').in('BELONGS_TO').outE('TRANSFER').values('amount').mean()",
                mapping
        );

        assertEquals(
                "SELECT AVG(e3.amount) AS mean FROM aml_countries v0 JOIN aml_bank_country e1 ON e1.country_id = v0.id JOIN aml_banks v1 ON v1.id = e1.bank_id JOIN aml_account_bank e2 ON e2.bank_id = v1.id JOIN aml_accounts v2 ON v2.id = e2.account_id JOIN aml_transfers e3 ON e3.from_account_id = v2.id WHERE v0.country_name = ?",
                result.sql()
        );
        assertEquals(List.of("Singapore"), result.parameters());
    }
}
