package com.jonathantong.StreamShift.model;

import java.util.List;

/**
 * Metadata for a database table
 */
public class TableMetadata {

    private final String tableName;
    private final List<String> primaryKeyColumns;

    public TableMetadata(String tableName, List<String> primaryKeyColumns) {
        this.tableName = tableName;
        this.primaryKeyColumns = primaryKeyColumns;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    public boolean hasPrimaryKey() {
        return primaryKeyColumns != null && !primaryKeyColumns.isEmpty();
    }

    @Override
    public String toString() {
        return "TableMetadata{" +
                "tableName='" + tableName + '\'' +
                ", primaryKeyColumns=" + primaryKeyColumns +
                '}';
    }
}