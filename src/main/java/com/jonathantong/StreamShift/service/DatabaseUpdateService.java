package com.jonathantong.StreamShift.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service for applying database changes to target PostgreSQL database
 */
@Service
@Transactional
public class DatabaseUpdateService {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseUpdateService.class);

    private final JdbcTemplate targetJdbcTemplate;

    @Autowired
    public DatabaseUpdateService(@Qualifier("targetJdbcTemplate") JdbcTemplate targetJdbcTemplate) {
        this.targetJdbcTemplate = targetJdbcTemplate;
    }

    /**
     * Insert a new record into the target table
     */
    public void insert(String tableName, Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            logger.warn("No data provided for INSERT into table: {}", tableName);
            return;
        }

        // Build INSERT statement
        List<String> columns = new ArrayList<>(data.keySet());
        List<Object> values = new ArrayList<>();

        String columnsList = columns.stream()
                .map(col -> "\"" + col + "\"") // Quote column names for PostgreSQL
                .collect(Collectors.joining(", "));

        String placeholders = columns.stream()
                .map(col -> "?")
                .collect(Collectors.joining(", "));

        // Prepare values in same order as columns
        for (String column : columns) {
            Object value = data.get(column);
            values.add(convertValue(value));
        }

        String sql = String.format("INSERT INTO \"%s\" (%s) VALUES (%s)",
                tableName, columnsList, placeholders);

        logger.debug("Executing INSERT: {} with values: {}", sql, values);

        try {
            int rowsAffected = targetJdbcTemplate.update(sql, values.toArray());
            logger.debug("INSERT successful: {} rows affected in table {}", rowsAffected, tableName);
        } catch (Exception e) {
            logger.error("Failed to INSERT into table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Insert failed for table " + tableName, e);
        }
    }

    /**
     * Update an existing record in the target table
     */
    public void update(String tableName, Map<String, Object> newData, Map<String, Object> whereClause) {
        if (newData == null || newData.isEmpty()) {
            logger.warn("No data provided for UPDATE in table: {}", tableName);
            return;
        }

        if (whereClause == null || whereClause.isEmpty()) {
            logger.error("No WHERE clause provided for UPDATE in table: {}", tableName);
            throw new IllegalArgumentException("WHERE clause required for UPDATE");
        }

        // Build UPDATE statement
        List<Object> values = new ArrayList<>();

        // SET clause
        String setClause = newData.keySet().stream()
                .map(col -> "\"" + col + "\" = ?")
                .collect(Collectors.joining(", "));

        // Add SET values
        for (String column : newData.keySet()) {
            Object value = newData.get(column);
            values.add(convertValue(value));
        }

        // WHERE clause
        String whereClauseStr = whereClause.keySet().stream()
                .map(col -> "\"" + col + "\" = ?")
                .collect(Collectors.joining(" AND "));

        // Add WHERE values
        for (String column : whereClause.keySet()) {
            Object value = whereClause.get(column);
            values.add(convertValue(value));
        }

        String sql = String.format("UPDATE \"%s\" SET %s WHERE %s",
                tableName, setClause, whereClauseStr);

        logger.debug("Executing UPDATE: {} with values: {}", sql, values);

        try {
            int rowsAffected = targetJdbcTemplate.update(sql, values.toArray());
            logger.debug("UPDATE successful: {} rows affected in table {}", rowsAffected, tableName);

            if (rowsAffected == 0) {
                logger.warn("UPDATE affected 0 rows in table {} - record may not exist", tableName);
            }
        } catch (Exception e) {
            logger.error("Failed to UPDATE table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Update failed for table " + tableName, e);
        }
    }

    /**
     * Delete a record from the target table
     */
    public void delete(String tableName, Map<String, Object> whereClause) {
        if (whereClause == null || whereClause.isEmpty()) {
            logger.error("No WHERE clause provided for DELETE from table: {}", tableName);
            throw new IllegalArgumentException("WHERE clause required for DELETE");
        }

        // Build DELETE statement
        List<Object> values = new ArrayList<>();

        String whereClauseStr = whereClause.keySet().stream()
                .map(col -> "\"" + col + "\" = ?")
                .collect(Collectors.joining(" AND "));

        // Add WHERE values
        for (String column : whereClause.keySet()) {
            Object value = whereClause.get(column);
            values.add(convertValue(value));
        }

        String sql = String.format("DELETE FROM \"%s\" WHERE %s", tableName, whereClauseStr);

        logger.debug("Executing DELETE: {} with values: {}", sql, values);

        try {
            int rowsAffected = targetJdbcTemplate.update(sql, values.toArray());
            logger.debug("DELETE successful: {} rows affected in table {}", rowsAffected, tableName);

            if (rowsAffected == 0) {
                logger.warn("DELETE affected 0 rows in table {} - record may not exist", tableName);
            }
        } catch (Exception e) {
            logger.error("Failed to DELETE from table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Delete failed for table " + tableName, e);
        }
    }

    /**
     * Handle UPSERT operation (INSERT ON CONFLICT UPDATE)
     */
    public void upsert(String tableName, Map<String, Object> data, List<String> conflictColumns) {
        if (data == null || data.isEmpty()) {
            logger.warn("No data provided for UPSERT into table: {}", tableName);
            return;
        }

        // Build UPSERT statement
        List<String> columns = new ArrayList<>(data.keySet());
        List<Object> values = new ArrayList<>();

        String columnsList = columns.stream()
                .map(col -> "\"" + col + "\"")
                .collect(Collectors.joining(", "));

        String placeholders = columns.stream()
                .map(col -> "?")
                .collect(Collectors.joining(", "));

        // Prepare values
        for (String column : columns) {
            Object value = data.get(column);
            values.add(convertValue(value));
        }

        // ON CONFLICT clause
        String conflictColumnsList = conflictColumns.stream()
                .map(col -> "\"" + col + "\"")
                .collect(Collectors.joining(", "));

        // UPDATE SET clause for conflict resolution
        String updateSetClause = columns.stream()
                .filter(col -> !conflictColumns.contains(col)) // Don't update conflict columns
                .map(col -> "\"" + col + "\" = EXCLUDED.\"" + col + "\"")
                .collect(Collectors.joining(", "));

        String sql = String.format(
                "INSERT INTO \"%s\" (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
                tableName, columnsList, placeholders, conflictColumnsList, updateSetClause);

        logger.debug("Executing UPSERT: {} with values: {}", sql, values);

        try {
            int rowsAffected = targetJdbcTemplate.update(sql, values.toArray());
            logger.debug("UPSERT successful: {} rows affected in table {}", rowsAffected, tableName);
        } catch (Exception e) {
            logger.error("Failed to UPSERT into table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Upsert failed for table " + tableName, e);
        }
    }

    /**
     * Convert values to appropriate types for PostgreSQL
     * Handles Debezium's timestamp formats (epoch microseconds and milliseconds)
     */
    private Object convertValue(Object value) {
        if (value == null) {
            return null;
        }

        // Handle timestamp values from Debezium
        if (value instanceof Long) {
            Long longValue = (Long) value;

            // Check if this looks like a timestamp
            if (longValue > 0) {
                try {
                    // If it's larger than milliseconds range, treat as microseconds
                    if (longValue > 9999999999999L) { // More than 13 digits = microseconds
                        // Convert microseconds to milliseconds
                        return Timestamp.from(Instant.ofEpochMilli(longValue / 1000));
                    }
                    // Otherwise treat as milliseconds
                    else if (longValue > 946684800000L) { // After year 2000 in milliseconds
                        return Timestamp.from(Instant.ofEpochMilli(longValue));
                    }
                } catch (Exception e) {
                    logger.warn("Failed to convert timestamp {}: {}", longValue, e.getMessage());
                    return value;
                }
            }
        }

        // Handle integer timestamps
        if (value instanceof Integer) {
            Integer intValue = (Integer) value;
            // Check if this could be epoch seconds (not milliseconds)
            if (intValue > 946684800 && intValue < 4102444800L) { // Jan 1, 2000 to Jan 1, 2100 in seconds
                try {
                    return Timestamp.from(Instant.ofEpochSecond(intValue));
                } catch (Exception e) {
                    logger.warn("Failed to convert timestamp {}: {}", intValue, e.getMessage());
                    return value;
                }
            }
        }

        // Handle timestamp strings from Debezium
        if (value instanceof String) {
            String strValue = (String) value;

            // Handle ISO timestamp strings
            if (strValue.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*")) {
                try {
                    return Timestamp.valueOf(LocalDateTime.parse(strValue.substring(0, 19)));
                } catch (Exception e) {
                    logger.warn("Failed to parse timestamp string {}: {}", strValue, e.getMessage());
                    return strValue;
                }
            }
        }

        return value;
    }
}