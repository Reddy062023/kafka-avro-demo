package com.demo;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class DlqConsumer {

    @KafkaListener(topics = "orders-dlq", groupId = "dlq-group")
    public void consume(Object message) {
        System.out.println("🚨 DLQ received failed message: " + message);
    }
}