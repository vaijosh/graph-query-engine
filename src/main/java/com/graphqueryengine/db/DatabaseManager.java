package com.graphqueryengine.db;

import com.graphqueryengine.config.DatabaseConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseManager {
    private final DatabaseConfig databaseConfig;

    public DatabaseManager(DatabaseConfig databaseConfig) {
        this.databaseConfig = databaseConfig;
        loadDriver(databaseConfig.driverClass());
    }

    public Connection connection() throws SQLException {
        String url  = databaseConfig.url();
        String user = databaseConfig.user();
        // If the URL already contains the "user" property (e.g. Trino ?user=admin),
        // avoid passing it again as a separate argument — Trino's JDBC driver throws
        // "Connection property user is passed both by URL and properties" otherwise.
        boolean userInUrl = url != null && url.contains("user=");
        if (userInUrl || user == null || user.isBlank()) {
            assert url != null;
            return DriverManager.getConnection(url);
        }
        assert url != null;
        return DriverManager.getConnection(url, user, databaseConfig.password());
    }

    private static void loadDriver(String driverClass) {
        if (driverClass == null || driverClass.isBlank()) {
            return;
        }
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Failed to load JDBC driver: " + driverClass, e);
        }
    }
}

