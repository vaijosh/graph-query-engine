package com.graphqueryengine.gremlin;

import com.graphqueryengine.TestGraphH2;
import com.graphqueryengine.gremlin.provider.GraphProvider;
import com.graphqueryengine.gremlin.provider.GraphProviderFactory;
import com.graphqueryengine.gremlin.provider.SqlGraphProvider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GraphProviderFactoryTest {

    @Test
    void sqlProviderCreatedForSqlName() {
        TestGraphH2 h2 = TestGraphH2.shared();
        GraphProvider provider = GraphProviderFactory.fromProviderName(
                "sql", h2.databaseManager(), h2.mappingStore());
        assertInstanceOf(SqlGraphProvider.class, provider);
        assertEquals("sql", provider.providerId());
    }

    @Test
    void unknownProviderThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                GraphProviderFactory.fromProviderName("dummy_name", null, null));
    }
}

