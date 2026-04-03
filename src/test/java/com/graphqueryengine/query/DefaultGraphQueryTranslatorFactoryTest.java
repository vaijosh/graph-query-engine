package com.graphqueryengine.query;

import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.api.GraphQueryTranslator;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.factory.DefaultGraphQueryTranslatorFactory;
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

