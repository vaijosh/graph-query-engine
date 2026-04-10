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
    void rejectsUnsupportedBackend() {
        DefaultGraphQueryTranslatorFactory factory = new DefaultGraphQueryTranslatorFactory("sparksql");

        IllegalStateException ex = assertThrows(IllegalStateException.class, factory::create);
        assertTrue(ex.getMessage().contains("Unsupported query translator backend"));
    }

    @Test
    void createsSqlTranslatorWithAntlrParser() {
        // parserMode arg is deprecated and ignored — ANTLR is always used
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
