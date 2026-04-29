package com.demo.transaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class SalesTransactionConsumer {

    private final RabbitTemplate        rabbitTemplate;
    private final ObjectMapper          objectMapper;
    private final TransactionRepository repository;

    public SalesTransactionConsumer(RabbitTemplate rabbitTemplate,
                                    ObjectMapper objectMapper,
                                    TransactionRepository repository) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper   = objectMapper;
        this.repository     = repository;
    }

    //@KafkaListener(topics = "sales-transactions", groupId = "filter-group")

// NEW:
@KafkaListener(topics = "sales-transactions",
               groupId = "filter-group",
               containerFactory = "stringKafkaListenerContainerFactory")
    public void consume(String message) {
        try {
            SalesTransaction txn =
                objectMapper.readValue(message, SalesTransaction.class);

            System.out.println("Filter received from Kafka: "
                + txn.getTransactionId() + " type=" + txn.getType());

            // ── CONTENT-BASED ROUTING ─────────────────────────────────
            if ("CASH".equalsIgnoreCase(txn.getType())) {

                System.out.println("CASH detected → routing to RabbitMQ: "
                    + txn.getTransactionId());

                txn.setStatus("PENDING_SAFE");
                repository.save(txn);

                // Send to RabbitMQ
                rabbitTemplate.convertAndSend(
                    "cash.exchange",
                    "cash.routing.key",
                    txn
                );

            } else {
                System.out.println("NON-CASH (" + txn.getType()
                    + ") → skipping G4S: " + txn.getTransactionId());

                txn.setStatus("PROCESSED");
                repository.save(txn);
            }

        } catch (Exception e) {
            System.err.println("Filter consumer error: " + e.getMessage());
            throw new RuntimeException(e); // goes to Kafka DLQ
        }
    }
}