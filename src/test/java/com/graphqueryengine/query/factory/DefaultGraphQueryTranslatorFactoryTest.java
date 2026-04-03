package com.graphqueryengine.query.factory;

import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.api.GraphQueryTranslator;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.translate.sql.IcebergSqlGraphQueryTranslator;
import com.graphqueryengine.query.translate.sql.StandardSqlGraphQueryTranslator;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DefaultGraphQueryTranslatorFactoryTest {

    @Test
    void createsSqlTranslatorForDefaultBackend() {
        DefaultGraphQueryTranslatorFactory factory = new DefaultGraphQueryTranslatorFactory("sql", "legacy");

        GraphQueryTranslator translator = factory.create();
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Person').values('name').limit(1)",
                sampleMapping()
        );

        assertInstanceOf(StandardSqlGraphQueryTranslator.class, translator);
        assertEquals("SELECT name AS name FROM people LIMIT 1", result.sql());
    }

    @Test
    void createsIcebergTranslatorWhenRequested() {
        DefaultGraphQueryTranslatorFactory factory = new DefaultGraphQueryTranslatorFactory("iceberg", "legacy");

        GraphQueryTranslator translator = factory.create();

        assertInstanceOf(IcebergSqlGraphQueryTranslator.class, translator);
    }

    @Test
    void icebergTranslatorUsesArrayJoinForFoldProjection() {
        DefaultGraphQueryTranslatorFactory factory = new DefaultGraphQueryTranslatorFactory("iceberg", "legacy");
        GraphQueryTranslator translator = factory.create();

        MappingConfig mapping = new MappingConfig(
                Map.of(
                        "Account", new VertexMapping("aml.accounts", "id", Map.of("accountId", "account_id", "bankId", "bank_id")),
                        "Bank", new VertexMapping("aml.banks", "id", Map.of("bankName", "bank_name"))
                ),
                Map.of(
                        "BELONGS_TO", new com.graphqueryengine.mapping.EdgeMapping("aml.account_bank", "id", "out_id", "in_id", Map.of())
                )
        );

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').project('accountId','bankName').by('accountId').by(out('BELONGS_TO').values('bankName').fold())",
                mapping
        );

        assertTrue(result.sql().contains("ARRAY_JOIN(ARRAY_AGG"));
        assertFalse(result.sql().contains("STRING_AGG("));
    }

    @Test
    void standardBackendAutoUsesIcebergDialectForCatalogQualifiedTables() {
        DefaultGraphQueryTranslatorFactory factory = new DefaultGraphQueryTranslatorFactory("sql", "legacy");
        GraphQueryTranslator translator = factory.create();

        MappingConfig mapping = new MappingConfig(
                Map.of(
                        "Account", new VertexMapping("aml.accounts", "id", Map.of("accountId", "account_id")),
                        "Bank", new VertexMapping("aml.banks", "id", Map.of("bankName", "bank_name"))
                ),
                Map.of(
                        "BELONGS_TO", new com.graphqueryengine.mapping.EdgeMapping("aml.account_bank", "id", "out_id", "in_id", Map.of())
                )
        );

        TranslationResult result = translator.translate(
                "g.V().hasLabel('Account').project('accountId','bankName').by('accountId').by(out('BELONGS_TO').values('bankName').fold())",
                mapping
        );

        assertTrue(result.sql().contains("ARRAY_JOIN(ARRAY_AGG"));
        assertFalse(result.sql().contains("STRING_AGG("));
    }

    @Test
    void rejectsUnsupportedBackend() {
        DefaultGraphQueryTranslatorFactory factory = new DefaultGraphQueryTranslatorFactory("sparksql", "legacy");

        IllegalStateException ex = assertThrows(IllegalStateException.class, factory::create);
        assertTrue(ex.getMessage().contains("Unsupported query translator backend"));
    }

    @Test
    void createsSqlTranslatorWithAntlrParser() {
        DefaultGraphQueryTranslatorFactory factory = new DefaultGraphQueryTranslatorFactory("sql", "antlr");

        GraphQueryTranslator translator = factory.create();
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Person').values('name').limit(1)",
                sampleMapping()
        );

        assertEquals("SELECT name AS name FROM people LIMIT 1", result.sql());
    }

    @Test
    void rejectsUnsupportedParserMode() {
        DefaultGraphQueryTranslatorFactory factory = new DefaultGraphQueryTranslatorFactory("sql", "peg");

        IllegalStateException ex = assertThrows(IllegalStateException.class, factory::create);
        assertTrue(ex.getMessage().contains("Unsupported query parser mode"));
    }

    @Test
    void antlrAndLegacyParsers_produceSameSql() {
        GraphQueryTranslator legacyTranslator = new DefaultGraphQueryTranslatorFactory("sql", "legacy").create();
        GraphQueryTranslator antlrTranslator = new DefaultGraphQueryTranslatorFactory("sql", "antlr").create();
        String gremlin = "g.V().hasLabel('Person').values('name').limit(1)";

        TranslationResult legacy = legacyTranslator.translate(gremlin, sampleMapping());
        TranslationResult antlr = antlrTranslator.translate(gremlin, sampleMapping());

        assertEquals(legacy.sql(), antlr.sql());
        assertEquals(legacy.parameters(), antlr.parameters());
    }

    private MappingConfig sampleMapping() {
        return new MappingConfig(
                Map.of("Person", new VertexMapping("people", "id", Map.of("name", "name"))),
                Map.of()
        );
    }
}


