package com.graphqueryengine.gremlin.provider;

/**
 * String constants for graph provider identifiers and transaction metadata.
 *
 * <p>Kept in the provider package so they are co-located with the classes that use them.
 */
public final class ProviderConstants {

    private ProviderConstants() {}

    // ── Provider IDs ──────────────────────────────────────────────────────────
    public static final String PROVIDER_SQL       = "sql";
    public static final String PROVIDER_SQL_MULTI = "sql-multi";

    // ── Transaction metadata ──────────────────────────────────────────────────
    public static final String TX_MODE_READ_ONLY   = "read-only";
    public static final String TX_STATUS_COMMITTED = "committed";
}

