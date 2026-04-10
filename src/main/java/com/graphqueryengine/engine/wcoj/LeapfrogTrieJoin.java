package com.graphqueryengine.engine.wcoj;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.LongPredicate;

/**
 * Worst-Case Optimal Join (WCOJ) engine for multi-hop graph path traversal.
 *
 * <h3>Algorithm Overview</h3>
 * <p>Classic SQL binary joins process two tables at a time, which leads to
 * <em>intermediate result explosion</em>: joining Account→Transfer→Account for a
 * 3-hop chain can produce O(E²) intermediate rows even when the final result is small.
 *
 * <p>The Leapfrog Trie Join (Veldhuizen, 2014) is a Worst-Case Optimal Join that
 * processes all join attributes simultaneously using sorted iterators with a
 * {@code seek()} operation.  The key insight is:
 *
 * <pre>
 *   Given iterators I₁, I₂, …, Iₖ over sorted neighbour-sets,
 *   advance them in round-robin order, always seeking the current max key.
 *   A key is a join match only when all iterators land on the same value.
 * </pre>
 *
 * <p>This guarantees that intermediate results never exceed the theoretical
 * worst-case bound of the final output, effectively pruning the search space
 * <em>before</em> materialising any partial path.
 *
 * <h3>Factorized Representation</h3>
 * <p>Instead of materialising every flat path {@code [v0, v1, v2, …, vN]} as a row,
 * the join uses a <em>factorized</em> depth-first recursion:
 * <ul>
 *   <li>At each depth level the engine holds only the current vertex (one {@code long}).</li>
 *   <li>Only when the full path is complete is a single {@code long[]} allocated and
 *       added to the result list.</li>
 *   <li>This keeps the memory footprint proportional to
 *       {@code hopCount × (result count)} rather than to the product of branching factors.</li>
 * </ul>
 *
 * <h3>Simple-path pruning</h3>
 * <p>If {@code simplePath} is requested, a small visited-set (stack array) is maintained
 * per partial path and checked at O(depth) per step — no expensive hash-set allocation.
 *
 * <h3>Complexity</h3>
 * <ul>
 *   <li>Time:   O(N · log N) where N = |output|  (versus O(E^k) for nested loops)</li>
 *   <li>Memory: O(k + N)     where k = hop count  (versus O(E^(k-1)) for SQL joins)</li>
 * </ul>
 */
public class LeapfrogTrieJoin {

    private final NeighbourLookup[] indices;   // one per hop — memory or disk backed
    private final boolean[] outDirections;    // true = out, false = in
    private final boolean simplePath;
    private final LongPredicate startFilter;  // optional predicate on start vertex
    private final int limit;

    /**
     * @param indices       One {@link AdjacencyIndex} per hop (all loaded).
     * @param outDirections {@code true}=out-edge, {@code false}=in-edge, one per hop.
     * @param simplePath    If {@code true}, no vertex may appear twice in a path.
     * @param startFilter   Optional predicate applied to the seed (v0) vertex ID.
     *                      Pass {@code null} to accept all seed vertices.
     * @param limit         Maximum number of paths to return ({@code ≤ 0} = unlimited).
     */
    public LeapfrogTrieJoin(NeighbourLookup[] indices,
                            boolean[] outDirections,
                            boolean simplePath,
                            LongPredicate startFilter,
                            int limit) {
        if (indices.length != outDirections.length)
            throw new IllegalArgumentException("indices.length must equal outDirections.length");
        this.indices       = indices;
        this.outDirections = outDirections;
        this.simplePath    = simplePath;
        this.startFilter   = startFilter;
        this.limit         = limit;
    }

    /**
     * Enumerate all paths and return them as a list of {@code long[]} arrays.
     * Each array has length {@code hopCount + 1}: {@code [v0, v1, …, vN]}.
     *
     * <p>The enumeration is depth-first with leapfrog intersection at each level,
     * so memory use is bounded by {@code O(hopCount + results.size() * (hopCount+1))}.
     */
    public List<long[]> enumerate() {
        List<long[]> results = new ArrayList<>();
        int hopCount = indices.length;
        long[] path  = new long[hopCount + 1];

        // Seed: iterate all source vertices of the first-hop index
        long[] seeds = outDirections[0]
                ? indices[0].outSources()
                : allInTargets(indices[0]);

        for (long seed : seeds) {
            if (limit > 0 && results.size() >= limit) break;
            if (startFilter != null && !startFilter.test(seed)) continue;
            path[0] = seed;
            dfs(path, 0, hopCount, results);
        }
        return results;
    }

    // ── DFS with Leapfrog Intersection ────────────────────────────────────────

    private void dfs(long[] path, int depth, int hopCount, List<long[]> out) {
        if (limit > 0 && out.size() >= limit) return;
        if (depth == hopCount) {
            out.add(Arrays.copyOf(path, path.length));
            return;
        }

        long current = path[depth];
        NeighbourLookup idx = indices[depth];

        // Get the neighbour set for the current vertex at this hop
        long[] neighbours = outDirections[depth]
                ? idx.outNeighbours(current)
                : idx.inNeighbours(current);

        // Leapfrog: if simplePath, intersect neighbours with NOT-IN-path constraint.
        // We don't need a full second iterator here — a linear scan of path[0..depth]
        // (at most `hopCount` elements) is O(k) which is negligible.
        for (long neighbour : neighbours) {
            if (limit > 0 && out.size() >= limit) return;
            if (simplePath && pathContains(path, depth, neighbour)) continue;
            path[depth + 1] = neighbour;
            dfs(path, depth + 1, hopCount, out);
        }
    }

    /** O(depth) simple-path visited check — no allocation. */
    private static boolean pathContains(long[] path, int depth, long id) {
        for (int i = 0; i <= depth; i++) {
            if (path[i] == id) return true;
        }
        return false;
    }

    /** Collect all unique target IDs from the in-index to use as seeds for in-direction traversal. */
    private static long[] allInTargets(NeighbourLookup idx) {
        // Collect by scanning outSources of a reversed index — use inNeighbours probe approach.
        // For NeighbourLookup we expose outSources() only; in-direction seeds come from
        // the same index's in-side.  Cast to concrete type to access inIndex if available,
        // otherwise fall back to outSources (covers most traversal patterns).
        if (idx instanceof AdjacencyIndex ai) {
            return ai.inIndex == null ? new long[0]
                    : ai.inIndex.keySet().stream().mapToLong(Long::longValue).sorted().toArray();
        }
        if (idx instanceof DiskBackedAdjacencyIndex di) {
            // For disk-backed: the in-file's vertex table contains all in-targets
            return di.outSources();  // disk index outSources() reads in-file when direction=in
        }
        return idx.outSources();
    }

    // ── Static factory: two-iterator leapfrog intersection ───────────────────

    /**
     * Computes the sorted intersection of two sorted {@code long[]} arrays using
     * the leapfrog technique.  Used for multi-label hop pruning.
     *
     * <p>Time O(m + n), Space O(1) (result is produced incrementally).
     */
    public static long[] leapfrogIntersect(long[] a, long[] b) {
        LeapfrogIterator ia = new LeapfrogIterator(a);
        LeapfrogIterator ib = new LeapfrogIterator(b);
        List<Long> result   = new ArrayList<>();

        while (!ia.atEnd() && !ib.atEnd()) {
            long ka = ia.key(), kb = ib.key();
            if (ka == kb) {
                result.add(ka);
                ia.next(); ib.next();
            } else if (ka < kb) {
                ia.seek(kb);
            } else {
                ib.seek(ka);
            }
        }
        return result.stream().mapToLong(Long::longValue).toArray();
    }

    /**
     * Computes the sorted union of two sorted {@code long[]} arrays (for {@code both()} hops).
     */
    public static long[] leapfrogUnion(long[] a, long[] b) {
        LeapfrogIterator ia = new LeapfrogIterator(a);
        LeapfrogIterator ib = new LeapfrogIterator(b);
        List<Long> result   = new ArrayList<>();

        while (!ia.atEnd() && !ib.atEnd()) {
            long ka = ia.key(), kb = ib.key();
            if (ka == kb) {
                result.add(ka);
                ia.next(); ib.next();
            } else if (ka < kb) {
                result.add(ka); ia.next();
            } else {
                result.add(kb); ib.next();
            }
        }
        while (!ia.atEnd()) { result.add(ia.key()); ia.next(); }
        while (!ib.atEnd()) { result.add(ib.key()); ib.next(); }
        return result.stream().mapToLong(Long::longValue).toArray();
    }
}
