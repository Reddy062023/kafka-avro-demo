package com.demo;

import com.demo.audit.OrderAuditService;
import com.demo.avro.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    private final OrderService      orderService;
    private final OrderAuditService auditService;

    public OrderConsumer(OrderService orderService,
                         OrderAuditService auditService) {
        this.orderService = orderService;
        this.auditService = auditService;
    }

    @KafkaListener(topics = "orders", groupId = "demo-group")
    public void consume(Order order) {

        System.out.println("Received order: " + order.getId());

        if (order.getAmount() < 0) {

            auditService.log(
                order.getId().toString(),
                "FAILED",
                null, "FAILED",
                order.getAmount(),
                order.getCurrency().toString(),
                "Invalid amount: " + order.getAmount()
            );

            throw new IllegalArgumentException(
                "Invalid amount: " + order.getAmount()
            );
        }

        OrderDocument doc = new OrderDocument(
            order.getId().toString(),
            order.getAmount(),
            order.getCurrency().toString(),
            order.getStatus().toString()
        );
        orderService.save(doc);

        auditService.log(
            order.getId().toString(),
            "CREATED",
            null,
            order.getStatus().toString(),
            order.getAmount(),
            order.getCurrency().toString(),
            "Order received and saved"
        );

        System.out.println("Order saved: MongoDB + PostgreSQL audit");
    }
}