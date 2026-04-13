package com.graphqueryengine.engine.wcoj;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import com.graphqueryengine.query.translate.sql.constant.SqlKeyword;

/**
 * Compressed Sparse Row (CSR) adjacency index for a single edge relation.
 *
 * <p>Stores two sorted arrays per direction:
 * <ul>
 *   <li>{@code outIndex} — {@code vertex → sorted long[] of out-neighbours}</li>
 *   <li>{@code inIndex}  — {@code vertex → sorted long[] of in-neighbours}</li>
 * </ul>
 *
 * <p>Loading is done lazily from JDBC on first access and cached until
 * {@link #invalidate()} is called.  A single edge table (e.g. {@code aml_transfers})
 * is read with {@code SELECT out_id, in_id FROM <table>} and the rows are bucketed
 * into the two maps in O(E) time.
 *
 * <p>Lookup is O(1) — the caller receives a {@code long[]} that is already sorted,
 * ready to hand to a {@link LeapfrogIterator}.
 */
public class AdjacencyIndex implements NeighbourLookup {

    private static final Logger LOG = Logger.getLogger(AdjacencyIndex.class.getName());
    private static final long[] EMPTY = new long[0];

    /** Relation key: "table:outCol:inCol" */
    private final String key;
    private final String table;
    private final String outColumn;
    private final String inColumn;

    // package-scoped for testing
    volatile Map<Long, long[]> outIndex;  // fromId → sorted[] toIds
    volatile Map<Long, long[]> inIndex;   // toId   → sorted[] fromIds
    volatile long[] sortedVertices;       // all vertex ids that appear as source

    public AdjacencyIndex(String table, String outColumn, String inColumn) {
        this.table     = table;
        this.outColumn = outColumn;
        this.inColumn  = inColumn;
        this.key       = table + ":" + outColumn + ":" + inColumn;
    }

    public String key() { return key; }

    // ── Loading ──────────────────────────────────────────────────────────────

    /**
     * Load (or reload) the index from JDBC.
     * Thread-safe: guarded by {@code synchronized}.
     */
    public synchronized void load(Connection conn) throws SQLException {
        LOG.info("[WCOJ] Loading adjacency index for " + table);
        String sql = SqlKeyword.SELECT + outColumn + ", " + inColumn + SqlKeyword.FROM + table;

        Map<Long, List<Long>> outTmp = new HashMap<>();
        Map<Long, List<Long>> inTmp  = new HashMap<>();

        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                long from = rs.getLong(1);
                long to   = rs.getLong(2);
                outTmp.computeIfAbsent(from, k -> new ArrayList<>()).add(to);
                inTmp .computeIfAbsent(to,   k -> new ArrayList<>()).add(from);
            }
        }

        outIndex = buildSorted(outTmp);
        inIndex  = buildSorted(inTmp);
        sortedVertices = outIndex.keySet().stream()
                .mapToLong(Long::longValue).sorted().toArray();

        LOG.info(String.format("[WCOJ] Index ready — %d edges, %d out-sources, %d in-targets",
                outTmp.values().stream().mapToInt(List::size).sum(),
                outIndex.size(), inIndex.size()));
    }

    public boolean isLoaded() {
        return outIndex != null;
    }

    public synchronized void invalidate() {
        outIndex = null;
        inIndex  = null;
        sortedVertices = null;
    }

    // ── Lookups ──────────────────────────────────────────────────────────────

    /** Returns sorted neighbour ids for an outgoing traversal from {@code vertexId}. */
    public long[] outNeighbours(long vertexId) {
        return outIndex.getOrDefault(vertexId, EMPTY);
    }

    /** Returns sorted neighbour ids for an incoming traversal to {@code vertexId}. */
    public long[] inNeighbours(long vertexId) {
        return inIndex.getOrDefault(vertexId, EMPTY);
    }

    /** Returns sorted array of all vertex IDs that have at least one outgoing edge. */
    public long[] outSources() {
        return sortedVertices != null ? sortedVertices : EMPTY;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static Map<Long, long[]> buildSorted(Map<Long, List<Long>> tmp) {
        Map<Long, long[]> result = new ConcurrentHashMap<>(tmp.size() * 2);
        for (Map.Entry<Long, List<Long>> e : tmp.entrySet()) {
            long[] arr = e.getValue().stream().mapToLong(Long::longValue).sorted().toArray();
            result.put(e.getKey(), arr);
        }
        return result;
    }
}

