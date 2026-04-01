package com.graphqueryengine.query;

import com.graphqueryengine.db.DatabaseManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class QueryExecutionService {
    private final DatabaseManager databaseManager;

    public QueryExecutionService(DatabaseManager databaseManager) {
        this.databaseManager = databaseManager;
    }

    public QueryResponse execute(TranslationResult translationResult) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();

        try (Connection connection = databaseManager.connection();
             PreparedStatement preparedStatement = connection.prepareStatement(translationResult.sql())) {

            for (int i = 0; i < translationResult.parameters().size(); i++) {
                preparedStatement.setObject(i + 1, translationResult.parameters().get(i));
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columns = metaData.getColumnCount();

                while (resultSet.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= columns; i++) {
                        row.put(metaData.getColumnLabel(i), resultSet.getObject(i));
                    }
                    rows.add(row);
                }
            }
        }

        return new QueryResponse(translationResult.sql(), translationResult.parameters(), rows, rows.size());
    }
}

