package com.jonathantong.StreamShift.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Simple model for Debezium change events
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ChangeEvent {

    @JsonProperty("before")
    private JsonNode before;

    @JsonProperty("after")
    private JsonNode after;

    @JsonProperty("source")
    private Source source;

    @JsonProperty("op")
    private String operation;

    @JsonProperty("ts_ms")
    private Long timestampMs;

    // Constructors
    public ChangeEvent() {}

    // Getters and Setters
    public JsonNode getBefore() { return before; }
    public void setBefore(JsonNode before) { this.before = before; }

    public JsonNode getAfter() { return after; }
    public void setAfter(JsonNode after) { this.after = after; }

    public Source getSource() { return source; }
    public void setSource(Source source) { this.source = source; }

    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }

    public Long getTimestampMs() { return timestampMs; }
    public void setTimestampMs(Long timestampMs) { this.timestampMs = timestampMs; }

    // Helper methods
    public String getTableName() {
        return source != null ? source.getTable() : null;
    }

    public String getDatabaseName() {
        return source != null ? source.getDb() : null;
    }

    /**
     * Simple source info
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Source {
        private String db;
        private String table;

        public Source() {}

        public String getDb() { return db; }
        public void setDb(String db) { this.db = db; }

        public String getTable() { return table; }
        public void setTable(String table) { this.table = table; }
    }
}