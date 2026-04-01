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
        return DriverManager.getConnection(databaseConfig.url(), databaseConfig.user(), databaseConfig.password());
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

