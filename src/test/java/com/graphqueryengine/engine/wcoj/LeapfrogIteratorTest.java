package com.graphqueryengine.engine.wcoj;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link LeapfrogIterator} and {@link LeapfrogTrieJoin} utility methods.
 */
class LeapfrogIteratorTest {

    // ── LeapfrogIterator ──────────────────────────────────────────────────────

    @Test
    void emptyArray_atEndImmediately() {
        LeapfrogIterator it = new LeapfrogIterator(new long[0]);
        assertTrue(it.atEnd());
    }

    @Test
    void singleElement_readableAndAdvancesToEnd() {
        LeapfrogIterator it = new LeapfrogIterator(new long[]{42L});
        assertFalse(it.atEnd());
        assertEquals(42L, it.key());
        it.next();
        assertTrue(it.atEnd());
    }

    @Test
    void seek_landsOnExactMatch() {
        long[] data = {1, 3, 5, 7, 9};
        LeapfrogIterator it = new LeapfrogIterator(data);
        it.seek(5);
        assertFalse(it.atEnd());
        assertEquals(5L, it.key());
    }

    @Test
    void seek_landsOnNextLarger() {
        long[] data = {1, 3, 5, 7, 9};
        LeapfrogIterator it = new LeapfrogIterator(data);
        it.seek(4);
        assertEquals(5L, it.key());
    }

    @Test
    void seek_pastEnd_setsAtEnd() {
        long[] data = {1, 3, 5};
        LeapfrogIterator it = new LeapfrogIterator(data);
        it.seek(100);
        assertTrue(it.atEnd());
    }

    @Test
    void seek_beforeCurrentPosition_staysAtCurrentOrForward() {
        long[] data = {1, 3, 5, 7, 9};
        LeapfrogIterator it = new LeapfrogIterator(data);
        it.seek(5); // pos → 2
        it.seek(3); // seek backward — binary search from current pos, so stays ≥ current
        // seek is always from current pos, so seeking back doesn't go backwards
        assertTrue(it.key() >= 3L);
    }

    // ── LeapfrogTrieJoin utility methods ─────────────────────────────────────

    @Test
    void leapfrogIntersect_commonElements() {
        long[] a = {1, 3, 5, 7, 9};
        long[] b = {3, 5, 6, 9, 11};
        long[] result = LeapfrogTrieJoin.leapfrogIntersect(a, b);
        assertArrayEquals(new long[]{3, 5, 9}, result);
    }

    @Test
    void leapfrogIntersect_noOverlap_empty() {
        long[] a = {1, 2, 3};
        long[] b = {4, 5, 6};
        long[] result = LeapfrogTrieJoin.leapfrogIntersect(a, b);
        assertEquals(0, result.length);
    }

    @Test
    void leapfrogIntersect_identical_arrays() {
        long[] a = {1, 2, 3, 4};
        long[] result = LeapfrogTrieJoin.leapfrogIntersect(a, a);
        assertArrayEquals(new long[]{1, 2, 3, 4}, result);
    }

    @Test
    void leapfrogUnion_mergesSorted() {
        long[] a = {1, 3, 5};
        long[] b = {2, 3, 4, 6};
        long[] result = LeapfrogTrieJoin.leapfrogUnion(a, b);
        assertArrayEquals(new long[]{1, 2, 3, 4, 5, 6}, result);
    }

    @Test
    void leapfrogUnion_oneEmpty() {
        long[] a = {1, 2, 3};
        long[] b = {};
        long[] result = LeapfrogTrieJoin.leapfrogUnion(a, b);
        assertArrayEquals(new long[]{1, 2, 3}, result);
    }

    // ── LeapfrogTrieJoin enumeration ─────────────────────────────────────────

    @Test
    void enumerate_twoHop_findsCorrectPaths() {
        // Graph: 1→2, 1→3, 2→4, 3→4
        // Expected 2-hop paths from all sources: [1,2,4], [1,3,4]
        AdjacencyIndex hop1 = buildIndex(new long[][]{{1,2},{1,3}});
        AdjacencyIndex hop2 = buildIndex(new long[][]{{2,4},{3,4}});

        LeapfrogTrieJoin join = new LeapfrogTrieJoin(
                new AdjacencyIndex[]{hop1, hop2},
                new boolean[]{true, true},
                false, null, 0);

        List<long[]> paths = join.enumerate();
        assertEquals(2, paths.size());
        // Both paths end at 4
        assertTrue(paths.stream().allMatch(p -> p[2] == 4L));
    }

    @Test
    void enumerate_simplePath_excludesCycles() {
        // Graph: 1→2, 2→1 (a cycle)
        AdjacencyIndex hop1 = buildIndex(new long[][]{{1,2},{2,1}});
        AdjacencyIndex hop2 = buildIndex(new long[][]{{1,2},{2,1}});

        LeapfrogTrieJoin join = new LeapfrogTrieJoin(
                new AdjacencyIndex[]{hop1, hop2},
                new boolean[]{true, true},
                true,  // simplePath=true
                null, 0);

        List<long[]> paths = join.enumerate();
        // [1,2,1] is cyclic (1 appears twice) → excluded
        // [2,1,2] is cyclic → excluded
        for (long[] path : paths) {
            assertEquals(3, path.length);
            // All elements must be distinct
            assertEquals(3, java.util.Arrays.stream(path).distinct().count());
        }
    }

    @Test
    void enumerate_withLimit_stopsEarly() {
        // Dense graph: every node connects to every other
        long[] nodes = {1, 2, 3, 4, 5};
        long[][] edges = buildComplete(nodes);
        AdjacencyIndex idx = buildIndex(edges);

        LeapfrogTrieJoin join = new LeapfrogTrieJoin(
                new AdjacencyIndex[]{idx},
                new boolean[]{true},
                false, null, 3);  // limit=3

        List<long[]> paths = join.enumerate();
        assertTrue(paths.size() <= 3, "Should respect limit");
    }

    @Test
    void enumerate_withStartFilter_onlyMatchingSeeds() {
        // Graph: 1→2, 3→4; filter: only start from vertex 1
        AdjacencyIndex idx = buildIndex(new long[][]{{1,2},{3,4}});

        LeapfrogTrieJoin join = new LeapfrogTrieJoin(
                new AdjacencyIndex[]{idx},
                new boolean[]{true},
                false,
                id -> id == 1L,   // startFilter: only vertex 1
                0);

        List<long[]> paths = join.enumerate();
        assertEquals(1, paths.size());
        assertEquals(1L, paths.get(0)[0]);
        assertEquals(2L, paths.get(0)[1]);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static AdjacencyIndex buildIndex(long[][] edges) {
        AdjacencyIndex idx = new AdjacencyIndex("test", "out_id", "in_id") {
            // Override to pre-populate without JDBC
        };
        java.util.Map<Long, java.util.List<Long>> outTmp = new java.util.HashMap<>();
        java.util.Map<Long, java.util.List<Long>> inTmp  = new java.util.HashMap<>();
        for (long[] e : edges) {
            outTmp.computeIfAbsent(e[0], k -> new java.util.ArrayList<>()).add(e[1]);
            inTmp .computeIfAbsent(e[1], k -> new java.util.ArrayList<>()).add(e[0]);
        }
        // Use reflection to set package-private fields
        try {
            var outField = AdjacencyIndex.class.getDeclaredField("outIndex");
            var inField  = AdjacencyIndex.class.getDeclaredField("inIndex");
            var svField  = AdjacencyIndex.class.getDeclaredField("sortedVertices");
            outField.setAccessible(true);
            inField .setAccessible(true);
            svField .setAccessible(true);

            java.util.Map<Long, long[]> outMap = new java.util.HashMap<>();
            java.util.Map<Long, long[]> inMap  = new java.util.HashMap<>();
            for (var e : outTmp.entrySet()) {
                outMap.put(e.getKey(), e.getValue().stream().mapToLong(Long::longValue).sorted().toArray());
            }
            for (var e : inTmp.entrySet()) {
                inMap.put(e.getKey(), e.getValue().stream().mapToLong(Long::longValue).sorted().toArray());
            }
            outField.set(idx, outMap);
            inField .set(idx, inMap);
            svField .set(idx, outMap.keySet().stream().mapToLong(Long::longValue).sorted().toArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return idx;
    }

    private static long[][] buildComplete(long[] nodes) {
        java.util.List<long[]> edges = new java.util.ArrayList<>();
        for (long a : nodes) for (long b : nodes) if (a != b) edges.add(new long[]{a, b});
        return edges.toArray(new long[0][]);
    }
}

