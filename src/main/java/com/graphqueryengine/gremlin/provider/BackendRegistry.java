package com.graphqueryengine.gremlin.provider;

import com.graphqueryengine.config.BackendConfig;
import com.graphqueryengine.config.DatabaseConfig;
import com.graphqueryengine.db.DatabaseManager;
import com.graphqueryengine.gremlin.GremlinExecutionService;
import com.graphqueryengine.mapping.MappingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Holds a named set of {@link GremlinExecutionService} instances — one per configured SQL
 * backend — so a single running engine can serve queries against multiple data sources.
 *
 * <p>The first backend registered is always the <em>default</em> and is used when a request
 * carries no {@code X-Backend-Id} header.
 *
 * <p>Populated at startup from {@link BackendConfig#listFromEnvironment()}.
 * New backends can be added at runtime via {@link #register(BackendConfig, MappingStore)}
 * without restarting the JVM — the registry is fully thread-safe.
 *
 * <h3>Example: startup configuration</h3>
 * <pre>{@code
 * BACKENDS='[
 *   {"id":"h2",      "url":"jdbc:h2:file:./data/graph;AUTO_SERVER=TRUE"},
 *   {"id":"iceberg", "url":"jdbc:trino://localhost:8080/iceberg/aml","user":"admin"}
 * ]'
 * }</pre>
 *
 * <h3>Example: runtime registration via API</h3>
 * <pre>{@code
 * POST /backends/register
 * {"id":"iceberg","url":"jdbc:trino://localhost:8080/iceberg/aml","user":"admin","password":""}
 * }</pre>
 */
public final class BackendRegistry implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BackendRegistry.class);

    /** Guards all reads and writes to {@code entries}. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Ordered map: insertion order preserved so first entry is the default.
     * All access must be guarded by {@link #lock}.
     */
    private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>();

    /** Public view of a registered backend (no password exposed). */
    public record Entry(String id, String url, DatabaseManager manager, GremlinExecutionService service) {}

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    private BackendRegistry() {}

    /**
     * Builds a registry from all backends returned by
     * {@link BackendConfig#listFromEnvironment()}, sharing the supplied {@link MappingStore}.
     */
    public static BackendRegistry fromEnvironment(MappingStore mappingStore) {
        return from(BackendConfig.listFromEnvironment(), mappingStore);
    }

    /**
     * Builds a registry from an explicit list of backend configs (useful in tests).
     */
    public static BackendRegistry from(List<BackendConfig> configs, MappingStore mappingStore) {
        BackendRegistry registry = new BackendRegistry();
        for (BackendConfig cfg : configs) {
            registry.register(cfg, mappingStore);
        }
        if (registry.entries.isEmpty()) {
            throw new IllegalStateException("BackendRegistry: at least one backend must be configured");
        }
        return registry;
    }

    // -------------------------------------------------------------------------
    // Runtime registration
    // -------------------------------------------------------------------------

    /**
     * Registers (or replaces) a backend at runtime — no JVM restart required.
     *
     * <p>If a backend with the same {@code id} already exists it is silently replaced.
     * The <em>default</em> backend (first registered) is never changed by a replacement.
     *
     * <p>This method is thread-safe; in-flight requests using the old entry complete
     * normally because {@link DatabaseManager} creates a new connection per call.
     *
     * @param cfg          backend configuration (id, url, credentials, driver)
     * @param mappingStore shared mapping store passed to the new {@link SqlGraphProvider}
     * @return the newly created {@link Entry}
     */
    public Entry register(BackendConfig cfg, MappingStore mappingStore) {
        SqlGraphProvider provider = GraphProviderFactory.fromBackendConfig(cfg, mappingStore);
        DatabaseManager dm = provider.databaseManager();
        GremlinExecutionService svc = new GremlinExecutionService(provider);
        Entry entry = new Entry(cfg.id(), cfg.url(), dm, svc);

        lock.writeLock().lock();
        try {
            entries.put(cfg.id(), entry);
        } finally {
            lock.writeLock().unlock();
        }

        LOG.info("Backend registered: id={} url={}", cfg.id(), cfg.url());
        return entry;
    }

    /**
     * Convenience overload — builds a {@link BackendConfig} from raw parameters and
     * delegates to {@link #register(BackendConfig, MappingStore)}.
     */
    public Entry register(String id, String url, String user, String password,
                          MappingStore mappingStore) {
        String driver = DatabaseConfig.inferDriverClass(url);
        return register(new BackendConfig(id, url, user, password, driver), mappingStore);
    }

    // -------------------------------------------------------------------------
    // Lookup  (all reads use read-lock for safety)
    // -------------------------------------------------------------------------

    /**
     * Returns the service for the given backend id, or {@link Optional#empty()} if not found.
     */
    public Optional<GremlinExecutionService> getById(String id) {
        lock.readLock().lock();
        try {
            Entry e = entries.get(id);
            return e == null ? Optional.empty() : Optional.of(e.service());
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Returns whether a backend with the given id is registered. */
    public boolean contains(String id) {
        lock.readLock().lock();
        try {
            return entries.containsKey(id);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the {@link DatabaseManager} for the given backend id,
     * or the default manager when id is {@code null} or not registered.
     */
    public DatabaseManager getManagerById(String id) {
        lock.readLock().lock();
        try {
            if (id != null) {
                Entry e = entries.get(id);
                if (e != null) return e.manager();
            }
            return entries.values().iterator().next().manager();
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Returns the default (first-registered) service. */
    public GremlinExecutionService getDefault() {
        lock.readLock().lock();
        try {
            return entries.values().iterator().next().service();
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Returns the default (first-registered) backend id. */
    public String defaultId() {
        lock.readLock().lock();
        try {
            return entries.keySet().iterator().next();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns an unmodifiable snapshot of all registered entries (id + url, no password).
     */
    public List<Entry> listEntries() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(entries.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    // -------------------------------------------------------------------------
    // AutoCloseable
    // -------------------------------------------------------------------------

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            entries.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
}


