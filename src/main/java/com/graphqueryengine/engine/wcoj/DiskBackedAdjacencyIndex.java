package com.graphqueryengine.engine.wcoj;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * Disk-backed adjacency index stored as a pair of memory-mapped CSR files.
 *
 * <h3>Why disk-backed?</h3>
 * {@link AdjacencyIndex} loads the entire edge table into JVM heap.  For large
 * tables (e.g. 1 M+ edges) this causes GC pressure and triggers the WCOJ→SQL
 * fallback.  This class writes the same data to {@code .out.csr} / {@code .in.csr}
 * files under a configurable per-mapping cache directory.  The OS page cache
 * handles eviction — only adjacency pages for vertices actually visited by a
 * query are kept in RAM; cold pages are swapped out automatically.
 *
 * <h3>On-disk CSR layout (one file per direction)</h3>
 * <pre>
 *  Header       (24 B): vertexCount(8) | edgeCount(8) | reserved(8)
 *  Vertex table (V×16 B): [vertexId(8) | edgeOffset(8)] — sorted by vertexId
 *  Edge array   (E×8  B): [neighbourId(8)] — sorted within each vertex's slice
 * </pre>
 *
 * <h3>Lookup</h3>
 * O(log V) binary search in vertex table → O(1) slice → O(degree) copy.
 * Thread-safe: uses {@link ByteBuffer#duplicate()} so each call gets its own
 * position cursor on the shared mapped buffer.
 *
 * <h3>Quota</h3>
 * The owning {@link AdjacencyIndexRegistry} tracks total bytes written to each
 * mapping's cache directory and refuses to build new indexes that would exceed
 * the configured per-mapping disk quota.
 */
public class DiskBackedAdjacencyIndex implements NeighbourLookup {

    private static final Logger LOG =
            Logger.getLogger(DiskBackedAdjacencyIndex.class.getName());

    // CSR layout constants
    static final int  HEADER_BYTES = 24;   // 3 × long
    static final int  VERTEX_ENTRY = 16;   // vertexId(8) + edgeOffset(8)
    private static final long[] EMPTY = new long[0];

    // ── Identity ───────────────────────────────────────────────────────────────
    private final String table;
    private final String outColumn;
    private final String inColumn;

    /** Stable file-system key derived from table+columns. */
    final String indexKey;

    // ── File paths ─────────────────────────────────────────────────────────────
    final Path outFile;
    final Path inFile;

    // ── Memory-mapped buffers (read-only after build, null before) ─────────────
    private volatile MappedByteBuffer outBuf;
    private volatile MappedByteBuffer inBuf;

    // ── Cached header values ───────────────────────────────────────────────────
    private long outVertexCount;
    private long inVertexCount;

    // ── Constructor ────────────────────────────────────────────────────────────

    DiskBackedAdjacencyIndex(String table, String outColumn, String inColumn,
                             Path cacheDir) throws IOException {
        this.table     = table;
        this.outColumn = outColumn;
        this.inColumn  = inColumn;

        Files.createDirectories(cacheDir);

        this.indexKey = (table + "_" + outColumn + "_" + inColumn)
                .replaceAll("[^A-Za-z0-9_]", "_");
        this.outFile = cacheDir.resolve(indexKey + ".out.csr");
        this.inFile  = cacheDir.resolve(indexKey + ".in.csr");
    }

    // ── Build ──────────────────────────────────────────────────────────────────

    /**
     * Reads the edge table from JDBC, writes CSR files, and memory-maps them.
     * No-ops if both files already exist (cached from a previous run).
     *
     * @return Approximate bytes written (0 if cache was reused).
     */
    synchronized long buildIfAbsent(Connection conn) throws SQLException, IOException {
        if (Files.exists(outFile) && Files.exists(inFile)) {
            LOG.info("[WCOJ-Disk] Reusing cached index: " + outFile.getFileName());
            mapFiles();
            return 0L;
        }

        LOG.info("[WCOJ-Disk] Building disk index for " + table + " …");
        long t0 = System.currentTimeMillis();

        // ── 1. Read all edges ──────────────────────────────────────────────────
        Map<Long, List<Long>> outTmp = new HashMap<>();
        Map<Long, List<Long>> inTmp  = new HashMap<>();
        long edgeCount = 0;

        try (PreparedStatement ps = conn.prepareStatement(
                     "SELECT " + outColumn + ", " + inColumn + " FROM " + table);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                long from = rs.getLong(1), to = rs.getLong(2);
                outTmp.computeIfAbsent(from, k -> new ArrayList<>()).add(to);
                inTmp .computeIfAbsent(to,   k -> new ArrayList<>()).add(from);
                edgeCount++;
            }
        }

        // ── 2. Write CSR files ─────────────────────────────────────────────────
        writeCsr(outFile, outTmp, edgeCount);
        writeCsr(inFile,  inTmp,  edgeCount);

        long bytesWritten = Files.size(outFile) + Files.size(inFile);
        LOG.info(String.format(
                "[WCOJ-Disk] Index built in %d ms  out=%s  in=%s",
                System.currentTimeMillis() - t0,
                humanBytes(Files.size(outFile)),
                humanBytes(Files.size(inFile))));

        mapFiles();
        return bytesWritten;
    }

    // ── CSR writer ─────────────────────────────────────────────────────────────

    private static void writeCsr(Path path,
                                  Map<Long, List<Long>> adj,
                                  long edgeCount) throws IOException {
        long[] vertices = adj.keySet().stream().mapToLong(Long::longValue).sorted().toArray();
        long vc       = vertices.length;
        long fileSize = HEADER_BYTES + vc * VERTEX_ENTRY + edgeCount * Long.BYTES;

        try (FileChannel fc = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            MappedByteBuffer buf = fc.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            buf.order(ByteOrder.LITTLE_ENDIAN);

            // Header
            buf.putLong(vc);
            buf.putLong(edgeCount);
            buf.putLong(0L);          // reserved

            // Vertex table: [vertexId | runningEdgeOffset]
            long offset = 0;
            for (long vid : vertices) {
                buf.putLong(vid);
                buf.putLong(offset);
                offset += adj.get(vid).size();
            }

            // Edge array: sorted neighbours per vertex
            for (long vid : vertices) {
                long[] arr = adj.get(vid).stream().mapToLong(Long::longValue).sorted().toArray();
                for (long n : arr) buf.putLong(n);
            }

            buf.force();
        }
    }

    // ── Memory-map ─────────────────────────────────────────────────────────────

    private void mapFiles() throws IOException {
        outBuf = mapRO(outFile);
        outBuf.order(ByteOrder.LITTLE_ENDIAN);
        outVertexCount = outBuf.getLong(0);

        inBuf  = mapRO(inFile);
        inBuf.order(ByteOrder.LITTLE_ENDIAN);
        inVertexCount = inBuf.getLong(0);
    }

    private static MappedByteBuffer mapRO(Path p) throws IOException {
        try (FileChannel fc = FileChannel.open(p, StandardOpenOption.READ)) {
            return fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
        }
    }

    // ── NeighbourLookup impl ───────────────────────────────────────────────────

    @Override
    public long[] outNeighbours(long vertexId) {
        return lookup(outBuf, outVertexCount, vertexId);
    }

    @Override
    public long[] inNeighbours(long vertexId) {
        return lookup(inBuf, inVertexCount, vertexId);
    }

    @Override
    public long[] outSources() {
        return allVertices(outBuf, outVertexCount);
    }

    // ── Binary search in CSR ───────────────────────────────────────────────────

    private static long[] lookup(MappedByteBuffer buf, long vc, long target) {
        if (buf == null || vc == 0) return EMPTY;

        long lo = 0, hi = vc - 1;
        while (lo <= hi) {
            long mid    = (lo + hi) >>> 1;
            int  vtxPos = (int)(HEADER_BYTES + mid * VERTEX_ENTRY);
            long vid    = buf.getLong(vtxPos);

            if (vid == target) {
                long edgeOffset = buf.getLong(vtxPos + 8);
                long nextOffset = (mid + 1 < vc)
                        ? buf.getLong((int)(HEADER_BYTES + (mid + 1) * VERTEX_ENTRY + 8))
                        : buf.getLong(8); // total edgeCount from header

                int degree = (int)(nextOffset - edgeOffset);
                if (degree <= 0) return EMPTY;

                long edgeArrayStart = HEADER_BYTES + vc * VERTEX_ENTRY;
                ByteBuffer slice = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
                slice.position((int)(edgeArrayStart + edgeOffset * Long.BYTES));

                long[] result = new long[degree];
                for (int i = 0; i < degree; i++) result[i] = slice.getLong();
                return result;

            } else if (vid < target) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return EMPTY;
    }

    private static long[] allVertices(MappedByteBuffer buf, long vc) {
        if (buf == null || vc == 0) return EMPTY;
        long[] result = new long[(int) vc];
        for (int i = 0; i < (int) vc; i++)
            result[i] = buf.getLong((int)(HEADER_BYTES + (long) i * VERTEX_ENTRY));
        return result;
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────────

    /** Deletes on-disk files, forcing rebuild on next {@link #buildIfAbsent}. */
    synchronized void delete() throws IOException {
        outBuf = null;
        inBuf  = null;
        Files.deleteIfExists(outFile);
        Files.deleteIfExists(inFile);
        LOG.info("[WCOJ-Disk] Deleted index for " + table);
    }

    boolean isBuilt() { return outBuf != null; }

    // ── Helpers ────────────────────────────────────────────────────────────────

    static String humanBytes(long b) {
        if (b < 1024)         return b + " B";
        if (b < 1 << 20)      return String.format("%.1f KB", b / 1024.0);
        if (b < 1 << 30)      return String.format("%.1f MB", b / (1024.0 * 1024));
        return                       String.format("%.2f GB", b / (1024.0 * 1024 * 1024));
    }
}

