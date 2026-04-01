package com.graphqueryengine.config;

public record DatabaseConfig(String url, String user, String password, String driverClass) {
    public static DatabaseConfig fromEnvironment() {
        String url = envOrDefault("DB_URL", "jdbc:h2:file:./data/graph;AUTO_SERVER=TRUE");
        String user = envOrDefault("DB_USER", "sa");
        String password = envOrDefault("DB_PASSWORD", "");
        String driverClass = envOrDefault("DB_DRIVER", "org.h2.Driver");
        return new DatabaseConfig(url, user, password, driverClass);
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}

