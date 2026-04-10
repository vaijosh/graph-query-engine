package com.graphqueryengine.config;

public record DatabaseConfig(String url, String user, String password, String driverClass) {
    public static DatabaseConfig fromEnvironment() {
        String url = envOrDefault("DB_URL", "jdbc:h2:file:./data/graph;AUTO_SERVER=TRUE");
        String user = envOrDefault("DB_USER", "sa");
        String password = envOrDefault("DB_PASSWORD", "");
        String driverClass = envOrDefault("DB_DRIVER", inferDriverClass(url));
        return new DatabaseConfig(url, user, password, driverClass);
    }

    /**
     * Infers the JDBC driver class name from the URL prefix so callers don't need
     * to set {@code DB_DRIVER} explicitly for known backends.
     *
     * <ul>
     *   <li>{@code jdbc:h2:}    → {@code org.h2.Driver}</li>
     *   <li>{@code jdbc:trino:} → {@code io.trino.jdbc.TrinoDriver}</li>
     * </ul>
     *
     * Returns an empty string for unknown prefixes, relying on JDBC SPI auto-registration.
     */
    public static String inferDriverClass(String url) {
        if (url == null) return "";
        if (url.startsWith("jdbc:h2:"))    return "org.h2.Driver";
        if (url.startsWith("jdbc:trino:")) return "io.trino.jdbc.TrinoDriver";
        return "";
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}

