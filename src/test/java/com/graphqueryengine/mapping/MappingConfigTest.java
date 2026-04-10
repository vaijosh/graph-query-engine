package com.graphqueryengine.mapping;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link MappingConfig} — backwards-compatibility constructors,
 * {@code backends} / {@code defaultDatasource} handling, and datasource resolution helpers.
 */
class MappingConfigTest {

    private static final VertexMapping ACCOUNTS =
            new VertexMapping("aml.accounts", "id", Map.of("accountId", "account_id"));
    private static final VertexMapping BANKS =
            new VertexMapping("aml.banks", "id", "iceberg", Map.of("bankId", "bank_id"));
    private static final EdgeMapping TRANSFER =
            new EdgeMapping("aml.transfers", "id", "out_id", "in_id", Map.of());

    // ── Backward-compatible constructor ───────────────────────────────────────

    @Test
    void twoArgConstructorProducesEmptyBackendsAndNullDatasource() {
        MappingConfig cfg = new MappingConfig(
                Map.of("Account", ACCOUNTS),
                Map.of("TRANSFER", TRANSFER)
        );

        assertTrue(cfg.backends().isEmpty(), "backends should default to empty map");
        assertNull(cfg.defaultDatasource(), "defaultDatasource should default to null");
        assertEquals(1, cfg.vertices().size());
        assertEquals(1, cfg.edges().size());
    }

    // ── Canonical constructor ─────────────────────────────────────────────────

    @Test
    void nullBackendsNormalisedToEmptyMap() {
        MappingConfig cfg = new MappingConfig(
                null, null,
                Map.of("Account", ACCOUNTS),
                Map.of()
        );

        assertNotNull(cfg.backends());
        assertTrue(cfg.backends().isEmpty());
    }

    @Test
    void blankDefaultDatasourceNormalisedToNull() {
        MappingConfig cfg = new MappingConfig(
                null, "   ",
                Map.of("Account", ACCOUNTS),
                Map.of()
        );

        assertNull(cfg.defaultDatasource());
    }

    @Test
    void defaultDatasourceTrimmed() {
        MappingConfig cfg = new MappingConfig(
                null, "  iceberg  ",
                Map.of("Account", ACCOUNTS),
                Map.of()
        );

        assertEquals("iceberg", cfg.defaultDatasource());
    }

    // ── datasourceForVertex ───────────────────────────────────────────────────

    @Test
    void datasourceForVertexReturnsVertexLevelDatasource() {
        MappingConfig cfg = new MappingConfig(
                null, "default-ds",
                Map.of("Account", ACCOUNTS, "Bank", BANKS),
                Map.of()
        );

        // Bank has datasource="iceberg" at the vertex level — should beat defaultDatasource
        assertEquals("iceberg", cfg.datasourceForVertex("Bank"));
    }

    @Test
    void datasourceForVertexFallsBackToDefaultDatasource() {
        MappingConfig cfg = new MappingConfig(
                null, "h2",
                Map.of("Account", ACCOUNTS),
                Map.of()
        );

        // Account has no datasource — falls back to defaultDatasource
        assertEquals("h2", cfg.datasourceForVertex("Account"));
    }

    @Test
    void datasourceForVertexReturnsNullWhenNoFallback() {
        MappingConfig cfg = new MappingConfig(
                Map.of("Account", ACCOUNTS),
                Map.of()
        );

        assertNull(cfg.datasourceForVertex("Account"));
    }

    @Test
    void datasourceForVertexReturnsNullForUnknownLabel() {
        MappingConfig cfg = new MappingConfig(
                null, "h2",
                Map.of("Account", ACCOUNTS),
                Map.of()
        );

        // Unknown label — no vertex entry, falls to defaultDatasource
        assertEquals("h2", cfg.datasourceForVertex("Unknown"));
    }

    // ── datasourceForEdge ─────────────────────────────────────────────────────

    @Test
    void datasourceForEdgeFallsBackToDefaultDatasource() {
        MappingConfig cfg = new MappingConfig(
                null, "iceberg",
                Map.of("Account", ACCOUNTS),
                Map.of("TRANSFER", TRANSFER)
        );

        // TRANSFER has no datasource field — should use defaultDatasource
        assertEquals("iceberg", cfg.datasourceForEdge("TRANSFER"));
    }

    @Test
    void datasourceForEdgeReturnsEdgeLevelDatasource() {
        // EdgeMapping with datasource set via full constructor
        EdgeMapping edgeDs = new EdgeMapping(
                "aml.flagged", "id", "out_id", "in_id",
                Map.of(), null, null, "iceberg-edge"
        );

        MappingConfig cfg = new MappingConfig(
                null, "h2",
                Map.of("Account", ACCOUNTS),
                Map.of("FLAGGED_BY", edgeDs)
        );

        assertEquals("iceberg-edge", cfg.datasourceForEdge("FLAGGED_BY"));
    }

    @Test
    void datasourceForEdgeReturnsNullWhenNoFallback() {
        MappingConfig cfg = new MappingConfig(
                Map.of("Account", ACCOUNTS),
                Map.of("TRANSFER", TRANSFER)
        );

        assertNull(cfg.datasourceForEdge("TRANSFER"));
    }

    // ── backends map ─────────────────────────────────────────────────────────

    @Test
    void backendsMapStoredCorrectly() {
        BackendConnectionConfig icebergCfg = new BackendConnectionConfig(
                "jdbc:trino://localhost:8080/iceberg", "admin", "", "io.trino.jdbc.TrinoDriver");
        MappingConfig cfg = new MappingConfig(
                Map.of("iceberg", icebergCfg),
                "iceberg",
                Map.of("Account", ACCOUNTS),
                Map.of()
        );

        assertEquals(1, cfg.backends().size());
        assertTrue(cfg.backends().containsKey("iceberg"));
        assertEquals("jdbc:trino://localhost:8080/iceberg", cfg.backends().get("iceberg").url());
        assertEquals("admin", cfg.backends().get("iceberg").user());
    }

    @Test
    void nullVerticesNormalisedToEmptyMap() {
        MappingConfig cfg = new MappingConfig(null, null, null, Map.of("TRANSFER", TRANSFER));
        assertNotNull(cfg.vertices());
        assertTrue(cfg.vertices().isEmpty());
    }

    @Test
    void nullEdgesNormalisedToEmptyMap() {
        MappingConfig cfg = new MappingConfig(null, null, Map.of("Account", ACCOUNTS), null);
        assertNotNull(cfg.edges());
        assertTrue(cfg.edges().isEmpty());
    }
}

