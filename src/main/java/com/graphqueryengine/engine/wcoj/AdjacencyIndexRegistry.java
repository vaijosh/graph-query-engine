package com.graphqueryengine.engine.wcoj;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.query.translate.sql.constant.SqlKeyword;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Registry of adjacency indices keyed by edge relation, with automatic
 * disk spill when an edge table exceeds the in-memory row limit.
 *
 * <h3>Two-tier storage</h3>
 * <ul>
 *   <li><b>Tier 1 — heap ({@link AdjacencyIndex})</b>: used when the edge table
 *       has ≤ {@code maxEdgesInMemory} rows.  Lookups are O(1) HashMap.</li>
 *   <li><b>Tier 2 — disk ({@link DiskBackedAdjacencyIndex})</b>: used for larger
 *       tables.  Written as a memory-mapped CSR file; the OS page cache keeps hot
 *       adjacency pages in RAM and evicts cold ones automatically.</li>
 * </ul>
 *
 * <h3>Per-mapping isolation</h3>
 * Each mapping ID (e.g. {@code "aml-iceberg"}, {@code "social-h2"}) gets its own
 * subdirectory under {@code rootCacheDir}.  This means:
 * <ul>
 *   <li>Different client connections using different mappings never share index files.</li>
 *   <li>A {@code --wipe} on one mapping does not affect others.</li>
 *   <li>Disk quota is enforced independently per mapping.</li>
 * </ul>
 *
 * <h3>Disk quota</h3>
 * The registry tracks the total bytes written to each mapping's cache directory.
 * If building a new disk index would exceed {@code diskQuotaBytesPerMapping}, the
 * registry logs a warning and returns {@code null} — the caller falls back to SQL
 * for that query (identical behaviour to the old size limit, but scoped per mapping).
 *
 * <h3>TTL-based expiry</h3>
 * Each index entry carries a load timestamp. When {@code ttlSeconds > 0}, any cached
 * entry older than {@code ttlSeconds} seconds is treated as stale: the in-memory index
 * is invalidated and reloaded from JDBC, and the disk index is rebuilt.  Set
 * {@code ttlSeconds = 0} to disable TTL (indices live until {@link #invalidateMapping}
 * is called explicitly).
 *
 * <h3>Configuration via system properties / env-vars</h3>
 * <pre>
 *   wcoj.cache.dir          Root cache directory  (default: &lt;cwd&gt;/wcoj-index-cache)
 *   wcoj.disk.quota.mb      Per-mapping quota in MB  (default: 2048 = 2 GB)
 *   WCOJ_INDEX_TTL_SECONDS  Index TTL in seconds     (default: 30; 0 = disabled)
 * </pre>
 */
public class AdjacencyIndexRegistry {

    private static final Logger LOG =
            Logger.getLogger(AdjacencyIndexRegistry.class.getName());

    // ── Defaults ───────────────────────────────────────────────────────────────

    /** Edge rows below this limit are kept in JVM heap (fast HashMap lookups). */
    public static final long DEFAULT_MAX_EDGES_IN_MEMORY = 5_000_000L;

    /** Default per-mapping disk quota: 2 GB. */
    public static final long DEFAULT_DISK_QUOTA_BYTES = 2L * 1024 * 1024 * 1024;

    /** Default index TTL: 30 seconds. */
    public static final long DEFAULT_TTL_SECONDS = 30L;

    /** Root cache directory — one subdirectory per mapping ID is created below it. */
    public static final Path DEFAULT_CACHE_DIR = Path.of(
            System.getProperty("wcoj.cache.dir",
                    System.getProperty("user.dir") + "/wcoj-index-cache"));

    // ── Configuration ──────────────────────────────────────────────────────────

    private final long maxEdgesInMemory;
    private final long diskQuotaBytesPerMapping;
    private final Path rootCacheDir;
    /**
     * How long (in seconds) a cached index is considered fresh.
     * {@code 0} means no expiry — indices are permanent until manually invalidated.
     */
    private final long ttlSeconds;

    // ── Per-mapping state ──────────────────────────────────────────────────────

    /**
     * In-memory indices: mappingId → (edgeKey → AdjacencyIndex).
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, AdjacencyIndex>>
            memCache = new ConcurrentHashMap<>();

    /**
     * Disk-backed indices: mappingId → (edgeKey → DiskBackedAdjacencyIndex).
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, DiskBackedAdjacencyIndex>>
            diskCache = new ConcurrentHashMap<>();

    /**
     * Running disk usage per mapping ID (bytes).
     */
    private final ConcurrentHashMap<String, AtomicLong>
            diskUsageBytes = new ConcurrentHashMap<>();

    /**
     * Last-loaded epoch-millis for in-memory indices: mappingId → (edgeKey → loadedAt).
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>
            memLoadTime = new ConcurrentHashMap<>();

    /**
     * Last-built epoch-millis for disk-backed indices: mappingId → (edgeKey → builtAt).
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>
            diskLoadTime = new ConcurrentHashMap<>();

    // ── Constructors ───────────────────────────────────────────────────────────

    public AdjacencyIndexRegistry() {
        this(DEFAULT_MAX_EDGES_IN_MEMORY, DEFAULT_DISK_QUOTA_BYTES, DEFAULT_CACHE_DIR, DEFAULT_TTL_SECONDS);
    }

    public AdjacencyIndexRegistry(long maxEdgesInMemory,
                                   long diskQuotaBytesPerMapping,
                                   Path rootCacheDir) {
        this(maxEdgesInMemory, diskQuotaBytesPerMapping, rootCacheDir, DEFAULT_TTL_SECONDS);
    }

    public AdjacencyIndexRegistry(long maxEdgesInMemory,
                                   long diskQuotaBytesPerMapping,
                                   Path rootCacheDir,
                                   long ttlSeconds) {
        this.maxEdgesInMemory        = maxEdgesInMemory;
        this.diskQuotaBytesPerMapping = diskQuotaBytesPerMapping;
        this.rootCacheDir            = rootCacheDir;
        this.ttlSeconds              = ttlSeconds;
        LOG.info(String.format(
                "[WCOJ] Registry configured — memLimit=%,d edges  diskQuota=%s/mapping  cacheDir=%s  ttl=%ds",
                maxEdgesInMemory,
                DiskBackedAdjacencyIndex.humanBytes(diskQuotaBytesPerMapping),
                rootCacheDir,
                ttlSeconds));
    }

    // ── Main API ───────────────────────────────────────────────────────────────

    /**
     * Returns a {@link NeighbourLookup} for {@code edgeLabel} in the active mapping.
     *
     * <ul>
     *   <li>If the edge table has ≤ {@code maxEdgesInMemory} rows → in-memory index.</li>
     *   <li>If larger and within disk quota → disk-backed CSR index.</li>
     *   <li>If larger and over disk quota → returns {@code null}; caller falls back to SQL.</li>
     * </ul>
     *
     * <p>If a TTL is configured and the cached entry is older than {@code ttlSeconds} seconds,
     * the entry is treated as stale: in-memory indices are reloaded from JDBC and disk
     * indices are rebuilt before being returned.
     *
     * @param conn       JDBC connection for loading (read-only).
     * @param config     Active mapping configuration.
     * @param edgeLabel  Gremlin edge label (e.g. {@code "TRANSFER"}).
     * @param mappingId  Active mapping ID — used to scope the disk cache directory
     *                   and quota counter (e.g. {@code "aml-iceberg"}).
     * @return Loaded index, or {@code null} if quota exceeded and disk unavailable.
     */
    public NeighbourLookup getOrLoad(Connection conn,
                                     MappingConfig config,
                                     String edgeLabel,
                                     String mappingId) throws SQLException {
        EdgeMapping em = config.edges().get(edgeLabel);
        if (em == null) return null;

        String edgeKey = em.table() + ":" + em.outColumn() + ":" + em.inColumn();

        // ── Tier 1: check in-memory cache ─────────────────────────────────────
        ConcurrentHashMap<String, AdjacencyIndex> mMap =
                memCache.computeIfAbsent(mappingId, k -> new ConcurrentHashMap<>());
        ConcurrentHashMap<String, Long> mTimes =
                memLoadTime.computeIfAbsent(mappingId, k -> new ConcurrentHashMap<>());

        AdjacencyIndex mem = mMap.get(edgeKey);
        if (mem != null && mem.isLoaded()) {
            if (isFresh(mTimes.get(edgeKey))) {
                return mem;  // still fresh
            }
            LOG.info(String.format("[WCOJ] TTL expired for in-memory index '%s' (mapping='%s') — reloading.",
                    edgeKey, mappingId));
            mem.invalidate();
        }

        // ── Check row count to decide tier ────────────────────────────────────
        long rowCount = countRows(conn, em.table());

        if (rowCount <= maxEdgesInMemory) {
            // Load into heap
            AdjacencyIndex idx = mMap.computeIfAbsent(edgeKey,
                    k -> new AdjacencyIndex(em.table(), em.outColumn(), em.inColumn()));
            synchronized (idx) {
                if (!idx.isLoaded()) {
                    LOG.info(String.format("[WCOJ] Loading %s into memory (%,d rows)", em.table(), rowCount));
                    idx.load(conn);
                    mTimes.put(edgeKey, System.currentTimeMillis());
                }
            }
            return idx;
        }

        // ── Tier 2: disk-backed ───────────────────────────────────────────────
        LOG.info(String.format(
                "[WCOJ] %s has %,d rows (> %,d memory limit) — checking disk cache for mapping '%s'",
                em.table(), rowCount, maxEdgesInMemory, mappingId));

        ConcurrentHashMap<String, DiskBackedAdjacencyIndex> dMap =
                diskCache.computeIfAbsent(mappingId, k -> new ConcurrentHashMap<>());
        ConcurrentHashMap<String, Long> dTimes =
                diskLoadTime.computeIfAbsent(mappingId, k -> new ConcurrentHashMap<>());

        // Compute required bytes for this index (estimate: 2 files × edgeCount × 8 B
        // + vertex table overhead — the actual build will be accurate)
        long estimatedBytes = rowCount * Long.BYTES * 4; // conservative: 4× for both dirs + vtx table

        AtomicLong usage = diskUsageBytes.computeIfAbsent(mappingId, k -> new AtomicLong(0L));

        DiskBackedAdjacencyIndex disk = dMap.get(edgeKey);
        if (disk != null && disk.isBuilt()) {
            if (isFresh(dTimes.get(edgeKey))) {
                return disk;  // still fresh
            }
            LOG.info(String.format("[WCOJ] TTL expired for disk index '%s' (mapping='%s') — rebuilding.",
                    edgeKey, mappingId));
            try { disk.delete(); } catch (IOException ignored) {}
            dMap.remove(edgeKey);
            dTimes.remove(edgeKey);
            usage.addAndGet(-estimatedBytes); // reclaim estimated quota so rebuild is allowed
            disk = null;
        }

        // Enforce quota for new builds
        if (disk == null) {
            long currentUsage = usage.get();
            if (currentUsage + estimatedBytes > diskQuotaBytesPerMapping) {
                LOG.warning(String.format(
                        "[WCOJ] Disk quota exceeded for mapping '%s': used=%s  estimated=%s  quota=%s. " +
                        "Falling back to SQL for edge '%s'.",
                        mappingId,
                        DiskBackedAdjacencyIndex.humanBytes(currentUsage),
                        DiskBackedAdjacencyIndex.humanBytes(estimatedBytes),
                        DiskBackedAdjacencyIndex.humanBytes(diskQuotaBytesPerMapping),
                        edgeLabel));
                return null;  // caller falls back to SQL
            }
        }

        // Build or reuse disk index
        Path mappingCacheDir = rootCacheDir.resolve(
                mappingId.replaceAll("[^A-Za-z0-9_\\-]", "_"));
        try {
            DiskBackedAdjacencyIndex newIdx = dMap.computeIfAbsent(edgeKey, k -> {
                try {
                    return new DiskBackedAdjacencyIndex(
                            em.table(), em.outColumn(), em.inColumn(), mappingCacheDir);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            if (!newIdx.isBuilt()) {
                long written = newIdx.buildIfAbsent(conn);
                if (written > 0) {
                    long newTotal = usage.addAndGet(written);
                    dTimes.put(edgeKey, System.currentTimeMillis());
                    LOG.info(String.format(
                            "[WCOJ-Disk] Mapping '%s' disk usage: %s / %s",
                            mappingId,
                            DiskBackedAdjacencyIndex.humanBytes(newTotal),
                            DiskBackedAdjacencyIndex.humanBytes(diskQuotaBytesPerMapping)));
                }
            }
            return newIdx;
        } catch (IOException | UncheckedIOException e) {
            throw new SQLException(
                    "Failed to build disk index for " + em.table() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Convenience overload that uses {@code "default"} as the mapping ID.
     * Preserves backward compatibility with callers that don't pass a mapping ID.
     */
    public NeighbourLookup getOrLoad(Connection conn,
                                     MappingConfig config,
                                     String edgeLabel) throws SQLException {
        return getOrLoad(conn, config, edgeLabel, "default");
    }

    // ── Invalidation ──────────────────────────────────────────────────────────

    /**
     * Invalidate all indices for a specific mapping (memory + disk files).
     * Call this after a mapping upload or data reload for that mapping.
     */
    public void invalidateMapping(String mappingId) {
        ConcurrentHashMap<String, AdjacencyIndex> mMap = memCache.remove(mappingId);
        if (mMap != null) mMap.values().forEach(AdjacencyIndex::invalidate);
        memLoadTime.remove(mappingId);

        ConcurrentHashMap<String, DiskBackedAdjacencyIndex> dMap = diskCache.remove(mappingId);
        if (dMap != null) {
            dMap.values().forEach(d -> { try { d.delete(); } catch (IOException ignored) {} });
        }
        diskLoadTime.remove(mappingId);

        diskUsageBytes.remove(mappingId);
        LOG.info("[WCOJ] Invalidated all indices for mapping: " + mappingId);
    }

    /**
     * Invalidate all indices across all mappings.
     */
    @SuppressWarnings("unused")
    public void invalidateAll() {
        memCache.keySet().forEach(this::invalidateMapping);
        LOG.info("[WCOJ] All adjacency indices invalidated.");
    }

    // ── Quota reporting ────────────────────────────────────────────────────────

    /** Returns current disk usage in bytes for the given mapping ID. */
    @SuppressWarnings("unused")
    public long diskUsageBytes(String mappingId) {
        AtomicLong v = diskUsageBytes.get(mappingId);
        return v != null ? v.get() : 0L;
    }

    /** Returns the configured per-mapping disk quota in bytes. */
    @SuppressWarnings("unused")
    public long diskQuotaBytesPerMapping() { return diskQuotaBytesPerMapping; }

    /** Returns the configured TTL in seconds (0 = disabled). */
    @SuppressWarnings("unused")
    public long ttlSeconds() { return ttlSeconds; }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Returns {@code true} if the entry loaded at {@code loadedAtMillis} is still within
     * the configured TTL window.  Always returns {@code true} when TTL is disabled
     * ({@code ttlSeconds == 0}) or when {@code loadedAtMillis} is {@code null}.
     */
    private boolean isFresh(Long loadedAtMillis) {
        if (ttlSeconds <= 0 || loadedAtMillis == null) return true;
        return (System.currentTimeMillis() - loadedAtMillis) <= ttlSeconds * 1000L;
    }

    private static long countRows(Connection conn, String table) throws SQLException {
        try (var ps = conn.prepareStatement(SqlKeyword.SELECT_COUNT + table);
             var rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }
}

