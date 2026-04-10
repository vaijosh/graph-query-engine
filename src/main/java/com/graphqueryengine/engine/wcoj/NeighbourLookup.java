package com.graphqueryengine.engine.wcoj;

/**
 * Abstraction over an adjacency index regardless of whether it is stored
 * in-memory ({@link AdjacencyIndex}) or on disk ({@link DiskBackedAdjacencyIndex}).
 *
 * <p>All methods must be safe for concurrent calls from multiple threads
 * without external synchronization.
 */
public interface NeighbourLookup {

    /**
     * Returns a <em>sorted</em> array of vertex IDs reachable from {@code vertexId}
     * by following an out-edge.  Returns an empty array if the vertex has no out-edges.
     */
    long[] outNeighbours(long vertexId);

    /**
     * Returns a <em>sorted</em> array of vertex IDs that have an in-edge pointing to
     * {@code vertexId}.  Returns an empty array if the vertex has no in-edges.
     */
    long[] inNeighbours(long vertexId);

    /**
     * Returns all source vertex IDs that have at least one out-edge, sorted ascending.
     * Used as the seed set when no start-vertex filter is specified.
     */
    long[] outSources();
}

