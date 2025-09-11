package com.jonathantong.StreamShift;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * StreamShift - Zero-downtime database migration using CDC
 *
 * MVP Features:
 * 	- Debezium CDC change capture
 * 	- Kafka streaming
 * 	- Basic change event processing
 * 	- Logging and monitoring
 */
@SpringBootApplication
@EnableKafka
public class StreamShiftApplication {

	public static void main(String[] args) {
		SpringApplication.run(StreamShiftApplication.class, args);
	}

}
