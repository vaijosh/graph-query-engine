package com.graphqueryengine.mapping;

final class TableReferenceResolver {
    private static final String ICEBERG_PREFIX = "iceberg:";

    private TableReferenceResolver() {
    }

    static String toSqlReference(String rawTable) {
        if (rawTable == null || rawTable.isBlank()) {
            throw new IllegalArgumentException("Mapping table must be non-empty");
        }

        String table = rawTable.trim();
        if (!table.startsWith(ICEBERG_PREFIX)) {
            return table;
        }

        String icebergTarget = table.substring(ICEBERG_PREFIX.length()).trim();
        if (icebergTarget.isEmpty()) {
            throw new IllegalArgumentException("Iceberg table reference must include a path or identifier after 'iceberg:'");
        }

        // Path-like targets use DuckDB-compatible iceberg_scan(...).
        if (isIcebergLocation(icebergTarget)) {
            return "iceberg_scan('" + icebergTarget.replace("'", "''") + "')";
        }

        // Identifier-like targets are emitted as-is for engines with Iceberg catalogs.
        return icebergTarget;
    }

    private static boolean isIcebergLocation(String icebergTarget) {
        String lower = icebergTarget.toLowerCase();
        return lower.startsWith("s3://")
                || lower.startsWith("gs://")
                || lower.startsWith("abfs://")
                || lower.startsWith("abfss://")
                || lower.startsWith("hdfs://")
                || lower.startsWith("file://")
                || lower.startsWith("/")
                || lower.startsWith("./")
                || lower.startsWith("../");
    }
}

