package com.demo;

import com.demo.avro.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // Called from OrderController (4 args)
    public void sendOrder(String id, double amount,
                          String currency, String status) {
        Order order = new Order();
        order.setId(id);
        order.setAmount(amount);
        order.setCurrency(currency);
        order.setStatus(status);

        kafkaTemplate.send("orders", id, order);
        System.out.println("Sent to Kafka: " + id);
    }

    // Called from tests (3 args) — uses default status CREATED
    public void sendOrder(String id, double amount, String currency) {
        sendOrder(id, amount, currency, "CREATED");
    }
}