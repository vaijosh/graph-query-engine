package com.graphqueryengine.gremlin;
import com.graphqueryengine.config.BackendConfig;
import com.graphqueryengine.gremlin.provider.BackendRegistry;
import com.graphqueryengine.mapping.MappingStore;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
/**
 * Unit tests for {@link BackendRegistry}.
 * Uses an in-memory H2 database so no external infrastructure is required.
 */
class BackendRegistryTest {
    private static final String H2_URL = "jdbc:h2:mem:registry-test-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1";
    private static BackendConfig h2Config(String id) {
        return new BackendConfig(id, H2_URL, "sa", "", "org.h2.Driver");
    }
    private static MappingStore emptyStore() {
        return new MappingStore();
    }
    // ── from(List) ────────────────────────────────────────────────────────────
    @Test
    void fromRegistersAllBackends() {
        BackendRegistry registry = BackendRegistry.from(
                List.of(h2Config("h2"), h2Config("h2b")),
                emptyStore()
        );
        assertEquals(2, registry.listEntries().size());
        assertTrue(registry.contains("h2"));
        assertTrue(registry.contains("h2b"));
    }
    @Test
    void fromEmptyListThrows() {
        assertThrows(IllegalStateException.class,
                () -> BackendRegistry.from(List.of(), emptyStore()));
    }
    // ── defaultId / getDefault ────────────────────────────────────────────────
    @Test
    void firstRegisteredBackendIsDefault() {
        BackendRegistry registry = BackendRegistry.from(
                List.of(h2Config("first"), h2Config("second")),
                emptyStore()
        );
        assertEquals("first", registry.defaultId());
        assertNotNull(registry.getDefault());
    }
    // ── getById ───────────────────────────────────────────────────────────────
    @Test
    void getByIdReturnsServiceForKnownId() {
        BackendRegistry registry = BackendRegistry.from(
                List.of(h2Config("h2")),
                emptyStore()
        );
        assertTrue(registry.getById("h2").isPresent());
    }
    @Test
    void getByIdReturnsEmptyForUnknownId() {
        BackendRegistry registry = BackendRegistry.from(
                List.of(h2Config("h2")),
                emptyStore()
        );
        assertTrue(registry.getById("nosuchbackend").isEmpty());
    }
    // ── register (runtime add / replace) ─────────────────────────────────────
    @Test
    void registerAddsNewBackend() {
        BackendRegistry registry = BackendRegistry.from(
                List.of(h2Config("h2")),
                emptyStore()
        );
        registry.register(h2Config("h2extra"), emptyStore());
        assertTrue(registry.contains("h2extra"));
        assertEquals(2, registry.listEntries().size());
    }
    @Test
    void registerReplacesExistingBackend() {
        BackendRegistry registry = BackendRegistry.from(
                List.of(h2Config("h2")),
                emptyStore()
        );
        BackendRegistry.Entry replaced = registry.register(h2Config("h2"), emptyStore());
        assertEquals("h2", replaced.id());
        assertEquals(1, registry.listEntries().size()); // still only one entry
    }
    @Test
    void registerConvenienceOverloadWorks() {
        BackendRegistry registry = BackendRegistry.from(
                List.of(h2Config("h2")),
                emptyStore()
        );
        registry.register("extra", H2_URL, "sa", "", emptyStore());
        assertTrue(registry.contains("extra"));
    }
    // ── getManagerById ────────────────────────────────────────────────────────
    @Test
    void getManagerByIdReturnsCorrectManager() {
        BackendRegistry registry = BackendRegistry.from(
                List.of(h2Config("h2")),
                emptyStore()
        );
        assertNotNull(registry.getManagerById("h2"));
    }
    @Test
    void getManagerByIdFallsBackToDefaultForUnknown() {
        BackendRegistry registry = BackendRegistry.from(
                List.of(h2Config("h2")),
                emptyStore()
        );
        // Unknown id → falls back to default
        assertNotNull(registry.getManagerById("unknown"));
    }
    @Test
    void getManagerByNullFallsBackToDefault() {
        BackendRegistry registry = BackendRegistry.from(
                List.of(h2Config("h2")),
                emptyStore()
        );
        assertNotNull(registry.getManagerById(null));
    }
    // ── listEntries snapshot ──────────────────────────────────────────────────
    @Test
    void listEntriesIsSnapshot() {
        BackendRegistry registry = BackendRegistry.from(
                List.of(h2Config("h2")),
                emptyStore()
        );
        List<BackendRegistry.Entry> before = registry.listEntries();
        registry.register(h2Config("extra"), emptyStore());
        List<BackendRegistry.Entry> after = registry.listEntries();
        assertEquals(1, before.size());
        assertEquals(2, after.size()); // snapshot is independent
    }
    // ── close ─────────────────────────────────────────────────────────────────
    @Test
    void closeEmptiesEntries() {
        BackendRegistry registry = BackendRegistry.from(
                List.of(h2Config("h2")),
                emptyStore()
        );
        registry.close();
        assertTrue(registry.listEntries().isEmpty());
    }
}
