package com.graphqueryengine.gremlin;
import com.graphqueryengine.config.BackendConfig;
import com.graphqueryengine.db.DatabaseManager;
import com.graphqueryengine.gremlin.provider.BackendRegistry;
import com.graphqueryengine.gremlin.provider.SqlGraphProvider;
import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.MappingStore;
import com.graphqueryengine.mapping.VertexMapping;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import javax.script.ScriptException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
/**
 * Integration-style tests for {@link SqlGraphProvider}.
 *
 * Uses an in-memory H2 database — no external infrastructure required.
 */
class SqlGraphProviderTest {
    private static final String H2_URL =
            "jdbc:h2:mem:sql-provider-test;DB_CLOSE_DELAY=-1";
    private DatabaseManager databaseManager;
    private MappingStore mappingStore;
    @BeforeEach
    void setUp() throws Exception {
        databaseManager = new DatabaseManager(
                new com.graphqueryengine.config.DatabaseConfig(H2_URL, "sa", "", "org.h2.Driver"));
        mappingStore = new MappingStore();
        // Create a minimal schema for Person vertices
        try (Connection conn = databaseManager.connection();
             Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS people (id BIGINT PRIMARY KEY, name VARCHAR(255))");
            st.execute("DELETE FROM people");
            st.execute("INSERT INTO people VALUES (1, 'Alice')");
            st.execute("INSERT INTO people VALUES (2, 'Bob')");
        }
    }
    private MappingConfig personMapping() {
        return new MappingConfig(
                Map.of("Person", new VertexMapping("people", "id", Map.of("name", "name"))),
                Map.of()
        );
    }
    // ── Single-backend mode ───────────────────────────────────────────────────
    @Test
    void providerIdIsSql() {
        SqlGraphProvider provider = new SqlGraphProvider(databaseManager, mappingStore);
        assertEquals("sql", provider.providerId());
    }
    @Test
    void executeReturnsSingleColumnResultsAsList() throws ScriptException {
        mappingStore.put("m1", "People", personMapping(), true);
        SqlGraphProvider provider = new SqlGraphProvider(databaseManager, mappingStore);
        GremlinExecutionResult result = provider.execute(
                "g.V().hasLabel('Person').values('name').limit(10)");
        assertNotNull(result);
        assertTrue(result.resultCount() > 0);
    }
    @Test
    void executeWithNoActiveMappingReturnsEmptyResult() throws ScriptException {
        // No mapping registered
        SqlGraphProvider provider = new SqlGraphProvider(databaseManager, mappingStore);
        GremlinExecutionResult result = provider.execute(
                "g.V().hasLabel('Person').limit(1)");
        assertNotNull(result);
        assertEquals(0, result.resultCount());
    }
    @Test
    void executeInTransactionWrapsResultsWithReadOnlyCommitted() throws ScriptException {
        mappingStore.put("m1", "People", personMapping(), true);
        SqlGraphProvider provider = new SqlGraphProvider(databaseManager, mappingStore);
        GremlinTransactionalExecutionResult result = provider.executeInTransaction(
                "g.V().hasLabel('Person').values('name').limit(10)");
        assertNotNull(result);
        assertEquals("read-only", result.transactionMode());
        assertEquals("committed", result.transactionStatus());
    }
    @Test
    void executeThrowsScriptExceptionForUnknownLabel() {
        mappingStore.put("m1", "People", personMapping(), true);
        SqlGraphProvider provider = new SqlGraphProvider(databaseManager, mappingStore);
        // "Car" is not in the mapping — translator throws IllegalArgumentException which
        // the provider wraps as ScriptException
        assertThrows(ScriptException.class, () ->
                provider.execute("g.V().hasLabel('Car').limit(1)"));
    }
    // ── Multi-backend mode ────────────────────────────────────────────────────
    @Test
    void multiBackendProviderIdIsSqlMulti() {
        BackendRegistry registry = BackendRegistry.from(
                List.of(new BackendConfig("h2", H2_URL, "sa", "", "org.h2.Driver")),
                mappingStore
        );
        SqlGraphProvider provider = new SqlGraphProvider(registry, mappingStore);
        assertEquals("sql-multi", provider.providerId());
    }
    @Test
    void multiBackendExecutesAgainstInferredBackend() throws ScriptException {
        BackendRegistry registry = BackendRegistry.from(
                List.of(new BackendConfig("h2", H2_URL, "sa", "", "org.h2.Driver")),
                mappingStore
        );
        mappingStore.put("m1", "People", personMapping(), true);
        SqlGraphProvider provider = new SqlGraphProvider(registry, mappingStore);
        GremlinExecutionResult result = provider.execute(
                "g.V().hasLabel('Person').values('name').limit(10)");
        assertNotNull(result);
        assertTrue(result.resultCount() > 0);
    }
    @Test
    void multiBackendThrowsScriptExceptionForUnregisteredDatasource() {
        BackendRegistry registry = BackendRegistry.from(
                List.of(new BackendConfig("h2", H2_URL, "sa", "", "org.h2.Driver")),
                mappingStore
        );
        // Mapping that declares a datasource "iceberg" that is NOT in the registry
        MappingConfig icebergMapping = new MappingConfig(
                null,
                "iceberg",
                Map.of("Account", new VertexMapping("aml.accounts", "id", Map.of("accountId", "account_id"))),
                Map.of()
        );
        mappingStore.put("ice", "Iceberg", icebergMapping, true);
        SqlGraphProvider provider = new SqlGraphProvider(registry, mappingStore);
        ScriptException ex = assertThrows(ScriptException.class, () ->
                provider.execute("g.V().hasLabel('Account').limit(1)"));
        assertTrue(ex.getMessage().contains("iceberg"),
                "Error should mention the missing backend id");
    }
}
