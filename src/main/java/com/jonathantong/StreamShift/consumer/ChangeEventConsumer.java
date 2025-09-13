package com.jonathantong.StreamShift.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jonathantong.StreamShift.model.ChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Minimal MVP Kafka consumer for Debezium change events
 *
 * Just logs what it receives to verify the pipeline works
 */
@Component
public class ChangeEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ChangeEventConsumer.class);
    private final ObjectMapper objectMapper;

    @Autowired
    public ChangeEventConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topicPattern = "dbserver1\\.inventory\\..*",
            groupId = "streamshift-consumer-group"
    )
    public void handleChangeEvent(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {

        try {
            logger.info("Received event from topic: {}", topic);

            // Parse the message
            ChangeEvent changeEvent = objectMapper.readValue(message, ChangeEvent.class);

            if (changeEvent != null) {
                // Just log the basics
                String operation = getOperationName(changeEvent.getOperation());
                String tableName = changeEvent.getTableName();

                logger.info("{} on table: {}", operation, tableName);

                // Log the data (simplified)
                if (changeEvent.getAfter() != null) {
                    logger.info("Data: {}", changeEvent.getAfter().toString());
                } else if (changeEvent.getBefore() != null) {
                    logger.info("Data: {}", changeEvent.getBefore().toString());
                }

                // After pipeline is working, apply changes to target database
            }

            acknowledgment.acknowledge();

        } catch (Exception e) {
            logger.error("Error processing message: {}", e.getMessage());
            acknowledgment.acknowledge(); // Still acknowledge to avoid infinite retries
        }
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