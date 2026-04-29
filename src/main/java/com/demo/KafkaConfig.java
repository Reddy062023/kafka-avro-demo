package com.demo;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    // Use Spring Boot's auto-configured KafkaTemplate directly
    // No need to redefine producer factories — Boot handles it
    @Bean
    public CommonErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            kafkaTemplate,
            (record, ex) -> {
                System.out.println("❌ Message failed, sending to DLQ");
                System.out.println("   Topic: " + record.topic());
                System.out.println("   Key: " + record.key());
                System.out.println("   Error: " + ex.getMessage());
                return new TopicPartition("orders-dlq", 0);
            }
        );

        // Retry once after 1 second, then send to DLQ
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 1L));
    }
}