package com.jonathantong.StreamShift.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jonathantong.StreamShift.model.ChangeEvent;
import com.jonathantong.StreamShift.service.DatabaseUpdateService;
import com.jonathantong.StreamShift.service.SchemaMetadataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Enhanced Kafka consumer that processes Debezium change events
 * and applies them to the target database
 */
@Component
public class ChangeEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ChangeEventConsumer.class);

    private final ObjectMapper objectMapper;
    private final DatabaseUpdateService databaseUpdateService;
    private final SchemaMetadataService schemaMetadataService;

    @Autowired
    public ChangeEventConsumer(
            ObjectMapper objectMapper,
            DatabaseUpdateService databaseUpdateService,
            SchemaMetadataService schemaMetadataService) {
        this.objectMapper = objectMapper;
        this.databaseUpdateService = databaseUpdateService;
        this.schemaMetadataService = schemaMetadataService;
    }

    @KafkaListener(
            topicPattern = "dbserver1\\.inventory\\..*",
            groupId = "streamshift-consumer-group"
    )
    public void handleChangeEvent(
            @Payload(required = false) String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {

        try {
            logger.info("Received event from topic: {}", topic);

            // Handle tombstone records (null payloads after DELETE operations)
            if (message == null || message.isEmpty()) {
                logger.debug("Received tombstone record, skipping processing");
                acknowledgment.acknowledge();
                return;
            }

            // Parse the message
            ChangeEvent changeEvent = objectMapper.readValue(message, ChangeEvent.class);

            if (changeEvent != null) {
                String operation = changeEvent.getOperation();
                String tableName = changeEvent.getTableName();
                String databaseName = changeEvent.getDatabaseName();

                logger.info("Processing {} operation on table: {}", getOperationName(operation), tableName);

                // Ensure target table exists
                schemaMetadataService.ensureTargetTableExists(databaseName, tableName, changeEvent);

                // Process the change event
                processChangeEvent(changeEvent);

                logger.info("Successfully processed {} operation", getOperationName(operation));
            }

            acknowledgment.acknowledge();

        } catch (Exception e) {
            logger.error("Error processing message from topic {}: {}", topic, e.getMessage(), e);
            // For production, you might want to send to a dead letter queue instead
            acknowledgment.acknowledge();
        }
    }

    private void processChangeEvent(ChangeEvent changeEvent) {
        String operation = changeEvent.getOperation();
        String tableName = changeEvent.getTableName();

        switch (operation) {
            case "c": // CREATE (INSERT)
                handleInsert(tableName, changeEvent);
                break;
            case "u": // UPDATE
                handleUpdate(tableName, changeEvent);
                break;
            case "d": // DELETE
                handleDelete(tableName, changeEvent);
                break;
            case "r": // READ (snapshot)
                handleInsert(tableName, changeEvent); // Treat snapshot as insert
                break;
            default:
                logger.warn("Unknown operation type: {}", operation);
        }
    }

    private void handleInsert(String tableName, ChangeEvent changeEvent) {
        JsonNode afterData = changeEvent.getAfter();
        if (afterData != null) {
            Map<String, Object> data = convertJsonToMap(afterData);

            // Use upsert to handle out-of-order events
            List<String> conflictColumns = schemaMetadataService
                    .extractPrimaryKeyValues(tableName, new HashMap<>(), data)
                    .keySet()
                    .stream()
                    .toList();

            if (!conflictColumns.isEmpty()) {
                databaseUpdateService.upsert(tableName, data, conflictColumns);
            } else {
                // Fallback to regular insert if no primary key
                databaseUpdateService.insert(tableName, data);
            }
        }
    }

    private void handleUpdate(String tableName, ChangeEvent changeEvent) {
        JsonNode afterData = changeEvent.getAfter();
        JsonNode beforeData = changeEvent.getBefore();

        if (afterData != null) {
            Map<String, Object> newData = convertJsonToMap(afterData);
            Map<String, Object> oldData = beforeData != null ? convertJsonToMap(beforeData) : new HashMap<>();

            // Extract primary key for WHERE clause
            Map<String, Object> whereClause = schemaMetadataService
                    .extractPrimaryKeyValues(tableName, oldData, newData);

            if (!whereClause.isEmpty()) {
                databaseUpdateService.update(tableName, newData, whereClause);
            } else {
                logger.warn("No primary key found for update operation on table: {}", tableName);
                // Fallback to upsert
                handleInsert(tableName, changeEvent);
            }
        }
    }

    private void handleDelete(String tableName, ChangeEvent changeEvent) {
        JsonNode beforeData = changeEvent.getBefore();

        if (beforeData != null) {
            Map<String, Object> oldData = convertJsonToMap(beforeData);

            // Extract primary key for WHERE clause
            Map<String, Object> whereClause = schemaMetadataService
                    .extractPrimaryKeyValues(tableName, oldData, new HashMap<>());

            if (!whereClause.isEmpty()) {
                databaseUpdateService.delete(tableName, whereClause);
            } else {
                logger.warn("No primary key found for delete operation on table: {}", tableName);
            }
        }
    }

    private Map<String, Object> convertJsonToMap(JsonNode jsonNode) {
        Map<String, Object> map = new HashMap<>();

        jsonNode.fields().forEachRemaining(entry -> {
            String key = entry.getKey();
            JsonNode value = entry.getValue();

            if (value.isNull()) {
                map.put(key, null);
            } else if (value.isBoolean()) {
                map.put(key, value.booleanValue());
            } else if (value.isInt()) {
                map.put(key, value.intValue());
            } else if (value.isLong()) {
                map.put(key, value.longValue());
            } else if (value.isDouble()) {
                map.put(key, value.doubleValue());
            } else {
                map.put(key, value.asText());
            }
        });

        return map;
    }

    private String getOperationName(String operation) {
        switch (operation) {
            case "c": return "INSERT";
            case "u": return "UPDATE";
            case "d": return "DELETE";
            case "r": return "SNAPSHOT";
            default: return "UNKNOWN";
        }
    }
}