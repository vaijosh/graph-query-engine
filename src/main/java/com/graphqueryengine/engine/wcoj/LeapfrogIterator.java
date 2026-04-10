package com.graphqueryengine.engine.wcoj;

/**
 * An iterator over a <em>sorted</em> array of {@code long} vertex IDs that supports
 * the two operations required by the Leapfrog Trie Join algorithm:
 *
 * <ul>
 *   <li>{@link #next()}  — advance to the next element</li>
 *   <li>{@link #seek(long)} — jump to the smallest element ≥ {@code key}</li>
 * </ul>
 *
 * <p>Both operations run in <em>O(log n)</em> via binary search on the backing
 * sorted array.  The amortised complexity over a full leapfrog join is
 * <em>O(N log N)</em> where N is the output size — strictly better than the
 * O(N²) worst-case of nested-loop joins for dense graphs.
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * long[] sorted = adjacencyIndex.outNeighbours(startId);
 * LeapfrogIterator it = new LeapfrogIterator(sorted);
 * while (!it.atEnd()) {
 *     long id = it.key();
 *     it.next();
 * }
 * }</pre>
 */
public final class LeapfrogIterator {

    private final long[] data;
    private int pos;

    public LeapfrogIterator(long[] sortedData) {
        this.data = sortedData;
        this.pos  = 0;
    }

    /** Returns {@code true} when all elements have been consumed. */
    public boolean atEnd() {
        return pos >= data.length;
    }

    /** Current element.  Must not be called when {@link #atEnd()} is {@code true}. */
    public long key() {
        return data[pos];
    }

    /** Advance to the next element.  No-op when at end. */
    public void next() {
        if (pos < data.length) pos++;
    }

    /**
     * Advance to the smallest element ≥ {@code searchKey}.
     * Uses binary search — O(log n).
     * After this call {@link #atEnd()} must be checked before calling {@link #key()}.
     */
    public void seek(long searchKey) {
        // Binary search for insertion point of searchKey
        int lo = pos, hi = data.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (data[mid] < searchKey) lo = mid + 1;
            else hi = mid;
        }
        pos = lo;
    }

    /** Remaining element count (for diagnostics). */
    public int remaining() {
        return data.length - pos;
    }
}

