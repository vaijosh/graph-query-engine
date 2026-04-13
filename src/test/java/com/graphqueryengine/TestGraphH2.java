package com.graphqueryengine;

import com.graphqueryengine.config.DatabaseConfig;
import com.graphqueryengine.db.DatabaseManager;
import com.graphqueryengine.engine.wcoj.AdjacencyIndexRegistry;
import com.graphqueryengine.engine.wcoj.WcojGraphProvider;
import com.graphqueryengine.gremlin.GremlinExecutionService;
import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.MappingStore;
import com.graphqueryengine.mapping.VertexMapping;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * Shared test helper that builds a minimal in-memory H2 graph for unit tests.
 *
 * <p>Schema (mirrors the AML demo mapping used throughout the engine):
 * <ul>
 *   <li>{@code test_accounts}   — Account vertices (id, account_id, account_type, tx_id, risk_score, opened_date)</li>
 *   <li>{@code test_transfers}  — TRANSFER edges   (id, out_id, in_id, amount, is_laundering)</li>
 * </ul>
 *
 * <p>A 10-hop chain is seeded: Account-1 → Account-2 → … → Account-11.
 * All edges are TRANSFER with sequential amounts (10, 20, …, 100).
 *
 * <p>Usage:
 * <pre>{@code
 * // Preferred: reuse the shared singleton — schema+seed run only once per JVM.
 * TestGraphH2 h2 = TestGraphH2.shared();
 * GremlinExecutionService svc = h2.wcojService();   // WCOJ provider (shared instance)
 * GremlinExecutionService svc = h2.sqlService();    // plain SQL provider
 * }</pre>
 */
public class TestGraphH2 {

    public static final String H2_URL =
            "jdbc:h2:mem:test-graph;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";

    private static final DatabaseConfig DB_CONFIG =
            new DatabaseConfig(H2_URL, "sa", "", "org.h2.Driver");

    // ── Shared singleton ──────────────────────────────────────────────────────

    /**
     * JVM-wide singleton. Schema creation, seed, and WCOJ index build all happen
     * exactly once no matter how many test classes call {@link #shared()}.
     * Guarded by class-loading synchronisation so it is thread-safe.
     */
    private static final class SingletonHolder {
        static final TestGraphH2 INSTANCE = new TestGraphH2();
    }

    /**
     * Returns the shared, pre-initialised {@link TestGraphH2} instance.
     *
     * <p>Calling this instead of {@code new TestGraphH2()} avoids repeating the
     * expensive schema-drop/create → seed → WCOJ-index-build cycle for every
     * test class and therefore makes the full test suite significantly faster.
     */
    public static TestGraphH2 shared() {
        return SingletonHolder.INSTANCE;
    }

    // ── Instance state ────────────────────────────────────────────────────────

    private final DatabaseManager databaseManager;
    private final MappingStore mappingStore;
    /** Lazily created once and reused by all callers of {@link #wcojService()}. */
    private GremlinExecutionService sharedWcojService;

    public TestGraphH2() {
        this.databaseManager = new DatabaseManager(DB_CONFIG);
        this.mappingStore    = new MappingStore();
        try {
            initSchema();
            seedChain();
        } catch (SQLException e) {
            throw new IllegalStateException("TestGraphH2 setup failed", e);
        }
        mappingStore.put("test", "TestGraph", buildMapping(), true);
    }

    // ── Providers ─────────────────────────────────────────────────────────────

    /**
     * Returns a {@link GremlinExecutionService} backed by the WCOJ engine + H2.
     *
     * <p>The service (and the underlying {@link AdjacencyIndexRegistry}) is built once
     * and reused across all calls on the same {@code TestGraphH2} instance, so the
     * expensive WCOJ index-build only happens once per JVM when used via {@link #shared()}.
     */
    public synchronized GremlinExecutionService wcojService() {
        if (sharedWcojService == null) {
            Path tmpCache = Path.of(System.getProperty("java.io.tmpdir"), "wcoj-test-cache");
            AdjacencyIndexRegistry registry = new AdjacencyIndexRegistry(
                    10_000_000L,           // keep everything in memory for tests
                    512L * 1024 * 1024,    // 512 MB disk quota (not used in-memory)
                    tmpCache
            );
            sharedWcojService = new GremlinExecutionService(
                    new WcojGraphProvider(databaseManager, mappingStore, registry));
        }
        return sharedWcojService;
    }

    /** Exposes the raw {@link DatabaseManager} for low-level test setup. */
    public DatabaseManager databaseManager() { return databaseManager; }

    /** Exposes the {@link MappingStore} so tests can add extra mappings. */
    public MappingStore mappingStore() { return mappingStore; }

    // ── Schema & Seed ─────────────────────────────────────────────────────────

    private void initSchema() throws SQLException {
        try (Connection conn = databaseManager.connection();
             Statement st = conn.createStatement()) {

            st.execute("DROP TABLE IF EXISTS test_transfers");
            st.execute("DROP TABLE IF EXISTS test_accounts");

            st.execute("""
                    CREATE TABLE test_accounts (
                        id           BIGINT PRIMARY KEY,
                        account_id   VARCHAR(50),
                        account_type VARCHAR(20),
                        tx_id        VARCHAR(50),
                        risk_score   DOUBLE,
                        opened_date  DATE
                    )""");

            st.execute("""
                    CREATE TABLE test_transfers (
                        id           BIGINT PRIMARY KEY,
                        out_id       BIGINT,
                        in_id        BIGINT,
                        amount       DOUBLE,
                        is_laundering VARCHAR(1)
                    )""");
        }
    }

    private void seedChain() throws SQLException {
        try (Connection conn = databaseManager.connection();
             Statement st = conn.createStatement()) {

            // 11 Account vertices (id = 1..11)
            for (int i = 1; i <= 11; i++) {
                String type  = (i % 2 == 0) ? "MERCHANT" : "PERSONAL";
                double risk  = i * 0.09; // 0.09 .. 0.99
                String date  = (i <= 5) ? "'2020-01-0" + i + "'" : "NULL";
                st.execute(String.format(
                        "INSERT INTO test_accounts VALUES (%d, 'ACC-%d', '%s', 'TXN-900%d', %f, %s)",
                        (long) i, i, type, i, risk, date));
            }

            // 10 TRANSFER edges: 1→2, 2→3, …, 10→11
            for (int i = 1; i <= 10; i++) {
                String laundering = (i == 3 || i == 7) ? "'1'" : "'0'"; // edges 3 and 7 flagged
                st.execute(String.format(
                        "INSERT INTO test_transfers VALUES (%d, %d, %d, %f, %s)",
                        (long)(1000 + i), (long) i, (long)(i + 1), i * 10.0, laundering));
            }
        }
    }

    private static MappingConfig buildMapping() {
        VertexMapping account = new VertexMapping(
                "test_accounts",
                "id",
                Map.of(
                        "accountId",   "account_id",
                        "accountType", "account_type",
                        "txId",        "tx_id",
                        "riskScore",   "risk_score",
                        "openedDate",  "opened_date"
                )
        );
        EdgeMapping transfer = new EdgeMapping(
                "test_transfers",
                "id",
                "out_id",
                "in_id",
                Map.of(
                        "amount",       "amount",
                        "isLaundering", "is_laundering"
                ),
                "Account",
                "Account"
        );
        return new MappingConfig(
                Map.of("Account", account),
                Map.of("TRANSFER", transfer)
        );
    }
}

