package com.graphqueryengine.query.factory;

import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.api.GraphQueryTranslator;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.translate.sql.IcebergSqlGraphQueryTranslator;
import com.graphqueryengine.query.translate.sql.StandardSqlGraphQueryTranslator;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultGraphQueryTranslatorFactoryTest {

    @Test
    void createsSqlTranslatorForDefaultBackend() {
        DefaultGraphQueryTranslatorFactory factory = new DefaultGraphQueryTranslatorFactory("sql");

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
        DefaultGraphQueryTranslatorFactory factory = new DefaultGraphQueryTranslatorFactory("iceberg");

        GraphQueryTranslator translator = factory.create();

        assertInstanceOf(IcebergSqlGraphQueryTranslator.class, translator);
    }

    @Test
    void icebergTranslatorUsesArrayJoinForFoldProjection() {
        DefaultGraphQueryTranslatorFactory factory = new DefaultGraphQueryTranslatorFactory("iceberg");
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
        DefaultGraphQueryTranslatorFactory factory = new DefaultGraphQueryTranslatorFactory("sql");
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
        DefaultGraphQueryTranslatorFactory factory = new DefaultGraphQueryTranslatorFactory("sparksql");

        IllegalStateException ex = assertThrows(IllegalStateException.class, factory::create);
        assertTrue(ex.getMessage().contains("Unsupported query translator backend"));
    }

    @Test
    void createsSqlTranslatorWithAntlrParser() {
        // parserMode arg deprecated — ANTLR is always used
        DefaultGraphQueryTranslatorFactory factory = new DefaultGraphQueryTranslatorFactory("sql");

        GraphQueryTranslator translator = factory.create();
        TranslationResult result = translator.translate(
                "g.V().hasLabel('Person').values('name').limit(1)",
                sampleMapping()
        );

        assertEquals("SELECT name AS name FROM people LIMIT 1", result.sql());
    }

    @Test
    void antlrParser_producesCorrectSql() {
        // Legacy parser removed — ANTLR is the only parser; verify it produces correct SQL
        GraphQueryTranslator translator = new DefaultGraphQueryTranslatorFactory("sql").create();
        String gremlin = "g.V().hasLabel('Person').values('name').limit(1)";

        TranslationResult result = translator.translate(gremlin, sampleMapping());

        assertEquals("SELECT name AS name FROM people LIMIT 1", result.sql());
    }

    private MappingConfig sampleMapping() {
        return new MappingConfig(
                Map.of("Person", new VertexMapping("people", "id", Map.of("name", "name"))),
                Map.of()
        );
    }
}
