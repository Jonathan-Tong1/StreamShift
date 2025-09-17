package com.jonathantong.StreamShift.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.jonathantong.StreamShift.model.ChangeEvent;
import com.jonathantong.StreamShift.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Service for managing schema metadata and ensuring target tables exist
 */
@Service
public class SchemaMetadataService {

    private static final Logger logger = LoggerFactory.getLogger(SchemaMetadataService.class);

    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate targetJdbcTemplate;

    // Cache for table metadata to avoid repeated database queries
    private final Map<String, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();

    @Autowired
    public SchemaMetadataService(
            @Qualifier("sourceJdbcTemplate") JdbcTemplate sourceJdbcTemplate,
            @Qualifier("targetJdbcTemplate") JdbcTemplate targetJdbcTemplate) {
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.targetJdbcTemplate = targetJdbcTemplate;
    }

    /**
     * Ensure target table exists, creating it if necessary
     */
    public void ensureTargetTableExists(String databaseName, String tableName, ChangeEvent changeEvent) {
        String fullTableName = databaseName + "." + tableName;

        if (tableMetadataCache.containsKey(fullTableName)) {
            return; // Table already processed
        }

        try {
            // Check if target table exists
            boolean targetExists = targetTableExists(tableName);

            if (!targetExists) {
                logger.info("Target table {} does not exist, creating...", tableName);
                createTargetTable(tableName, changeEvent);
            }

            // Cache table metadata
            TableMetadata metadata = loadTableMetadata(tableName);
            tableMetadataCache.put(fullTableName, metadata);

            logger.info("Target table {} is ready", tableName);

        } catch (Exception e) {
            logger.error("Failed to ensure target table {} exists: {}", tableName, e.getMessage(), e);
            throw new RuntimeException("Failed to prepare target table " + tableName, e);
        }
    }

    /**
     * Extract primary key values from record data
     */
    public Map<String, Object> extractPrimaryKeyValues(String tableName, Map<String, Object> beforeData, Map<String, Object> afterData) {
        TableMetadata metadata = getTableMetadata(tableName);
        List<String> primaryKeys = metadata.getPrimaryKeyColumns();

        Map<String, Object> pkValues = new HashMap<>();

        // Use 'after' data first, fall back to 'before' data
        Map<String, Object> sourceData = !afterData.isEmpty() ? afterData : beforeData;

        for (String pkColumn : primaryKeys) {
            Object value = sourceData.get(pkColumn);
            if (value != null) {
                pkValues.put(pkColumn, value);
            }
        }

        if (pkValues.isEmpty()) {
            logger.warn("No primary key values found for table {}", tableName);
        }

        return pkValues;
    }

    /**
     * Check if target table exists
     */
    private boolean targetTableExists(String tableName) {
        String sql = """
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = ?
            """;

        Integer count = targetJdbcTemplate.queryForObject(sql, Integer.class, tableName);
        return count != null && count > 0;
    }

    /**
     * Create target table by analyzing the change event structure
     */
    private void createTargetTable(String tableName, ChangeEvent changeEvent) {
        // Analyze the change event to determine table structure
        JsonNode afterData = changeEvent.getAfter();
        JsonNode beforeData = changeEvent.getBefore();

        // Use whichever data is available
        JsonNode sampleData = afterData != null ? afterData : beforeData;

        if (sampleData == null) {
            throw new RuntimeException("No sample data available to create table " + tableName);
        }

        // Try to get actual schema from source database first
        try {
            createTableFromSourceSchema(tableName);
            return;
        } catch (Exception e) {
            logger.warn("Could not create table from source schema, using sample data approach: {}", e.getMessage());
        }

        // Fallback: Create table based on sample data
        createTableFromSampleData(tableName, sampleData);
    }

    /**
     * Create table by replicating source database schema
     */
    private void createTableFromSourceSchema(String tableName) {
        // Get column information from source database
        String columnInfoSql = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_name = ?
            ORDER BY ordinal_position
            """;

        List<Map<String, Object>> columns = sourceJdbcTemplate.queryForList(columnInfoSql, tableName);

        if (columns.isEmpty()) {
            throw new RuntimeException("No column information found for source table " + tableName);
        }

        // Get primary key information
        String pkSql = """
            SELECT column_name
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name
            WHERE tc.table_name = ? AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """;

        List<String> primaryKeys = sourceJdbcTemplate.queryForList(pkSql, String.class, tableName);

        // Build CREATE TABLE statement
        StringBuilder createTableSql = new StringBuilder("CREATE TABLE IF NOT EXISTS" +
                "" +
                " \"").append(tableName).append("\" (");

        List<String> columnDefinitions = new ArrayList<>();

        for (Map<String, Object> column : columns) {
            String columnName = (String) column.get("column_name");
            String dataType = (String) column.get("data_type");
            String isNullable = (String) column.get("is_nullable");
            Object maxLength = column.get("character_maximum_length");
            Object precision = column.get("numeric_precision");
            Object scale = column.get("numeric_scale");

            StringBuilder colDef = new StringBuilder("\"").append(columnName).append("\" ");

            // Map PostgreSQL data types
            String pgDataType = mapDataType(dataType, maxLength, precision, scale);
            colDef.append(pgDataType);

            if ("NO".equals(isNullable)) {
                colDef.append(" NOT NULL");
            }

            columnDefinitions.add(colDef.toString());
        }

        createTableSql.append(String.join(", ", columnDefinitions));

        // Add primary key constraint
        if (!primaryKeys.isEmpty()) {
            createTableSql.append(", PRIMARY KEY (");
            createTableSql.append(primaryKeys.stream()
                    .map(pk -> "\"" + pk + "\"")
                    .collect(Collectors.joining(", ")));
            createTableSql.append(")");
        }

        createTableSql.append(")");

        logger.info("Creating target table: {}", createTableSql);
        targetJdbcTemplate.execute(createTableSql.toString());
    }

    /**
     * Create table based on sample data (fallback approach)
     */
    private void createTableFromSampleData(String tableName, JsonNode sampleData) {
        StringBuilder createTableSql = new StringBuilder("CREATE TABLE IF NOT EXISTS \"").append(tableName).append("\" (");

        List<String> columnDefinitions = new ArrayList<>();

        sampleData.fields().forEachRemaining(entry -> {
            String columnName = entry.getKey();
            JsonNode value = entry.getValue();

            String dataType = inferDataType(value);
            columnDefinitions.add("\"" + columnName + "\" " + dataType);
        });

        createTableSql.append(String.join(", ", columnDefinitions));
        createTableSql.append(")");

        logger.info("Creating target table from sample data: {}", createTableSql);
        targetJdbcTemplate.execute(createTableSql.toString());

        logger.warn("Table {} created from sample data - consider adding proper constraints later", tableName);
    }

    /**
     * Load table metadata including primary keys
     */
    private TableMetadata loadTableMetadata(String tableName) {
        // Get primary key columns
        String pkSql = """
            SELECT column_name
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name
            WHERE tc.table_name = ? AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """;

        List<String> primaryKeys = targetJdbcTemplate.queryForList(pkSql, String.class, tableName);

        return new TableMetadata(tableName, primaryKeys);
    }

    /**
     * Get cached table metadata
     */
    private TableMetadata getTableMetadata(String tableName) {
        // Look for cached metadata (try with different prefixes)
        for (String key : tableMetadataCache.keySet()) {
            if (key.endsWith("." + tableName)) {
                return tableMetadataCache.get(key);
            }
        }

        // If not found, load it
        return loadTableMetadata(tableName);
    }

    /**
     * Map PostgreSQL data types
     */
    private String mapDataType(String sourceType, Object maxLength, Object precision, Object scale) {
        switch (sourceType.toLowerCase()) {
            case "varchar":
            case "character varying":
                return maxLength != null ? "VARCHAR(" + maxLength + ")" : "TEXT";
            case "char":
            case "character":
                return maxLength != null ? "CHAR(" + maxLength + ")" : "CHAR(1)";
            case "text":
                return "TEXT";
            case "integer":
            case "int":
            case "int4":
                return "INTEGER";
            case "bigint":
            case "int8":
                return "BIGINT";
            case "smallint":
            case "int2":
                return "SMALLINT";
            case "decimal":
            case "numeric":
                if (precision != null && scale != null) {
                    return "NUMERIC(" + precision + "," + scale + ")";
                } else if (precision != null) {
                    return "NUMERIC(" + precision + ")";
                }
                return "NUMERIC";
            case "real":
            case "float4":
                return "REAL";
            case "double precision":
            case "float8":
                return "DOUBLE PRECISION";
            case "boolean":
            case "bool":
                return "BOOLEAN";
            case "date":
                return "DATE";
            case "time":
                return "TIME";
            case "timestamp":
            case "timestamp without time zone":
                return "TIMESTAMP";
            case "timestamp with time zone":
                return "TIMESTAMPTZ";
            case "json":
                return "JSON";
            case "jsonb":
                return "JSONB";
            case "uuid":
                return "UUID";
            default:
                logger.warn("Unknown data type {}, using TEXT", sourceType);
                return "TEXT";
        }
    }

    /**
     * Infer data type from JSON value (fallback approach)
     */
    private String inferDataType(JsonNode value) {
        if (value.isNull()) {
            return "TEXT"; // Default for null values
        } else if (value.isBoolean()) {
            return "BOOLEAN";
        } else if (value.isInt()) {
            return "INTEGER";
        } else if (value.isLong()) {
            return "BIGINT";
        } else if (value.isDouble() || value.isFloat()) {
            return "DOUBLE PRECISION";
        } else if (value.isTextual()) {
            String text = value.textValue();
            // Check if it looks like a timestamp
            if (text.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*")) {
                return "TIMESTAMP";
            }
            // Check if it looks like a date
            if (text.matches("\\d{4}-\\d{2}-\\d{2}")) {
                return "DATE";
            }
            return "TEXT";
        } else {
            return "TEXT"; // Default for complex types
        }
    }
}