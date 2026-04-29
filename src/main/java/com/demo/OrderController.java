package com.demo;

import com.demo.audit.OrderAuditLog;
import com.demo.audit.OrderAuditService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderProducer     producer;
    private final OrderService      orderService;
    private final OrderAuditService auditService;

    public OrderController(OrderProducer producer,
                           OrderService orderService,
                           OrderAuditService auditService) {
        this.producer     = producer;
        this.orderService = orderService;
        this.auditService = auditService;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> sendOrder(
            @Valid @RequestBody OrderRequest request) {

        producer.sendOrder(
            request.getId(),
            request.getAmount(),
            request.getCurrency(),
            request.getStatus()
        );

        return ResponseEntity.status(HttpStatus.CREATED).body(
            new OrderResponse(
                request.getId(),
                request.getAmount(),
                request.getCurrency(),
                request.getStatus()
            )
        );
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderDocument> getOrder(@PathVariable String id) {
        Optional<OrderDocument> order = orderService.findById(id);
        return order.map(ResponseEntity::ok)
                    .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public ResponseEntity<Map<String, Object>> getAllOrders() {
        List<OrderDocument> orders = orderService.findAll();
        Map<String, Object> response = new HashMap<>();
        response.put("orders", orders);
        response.put("total",  orders.size());
        return ResponseEntity.ok(response);
    }

    @GetMapping("/status/{status}")
    public ResponseEntity<List<OrderDocument>> getByStatus(
            @PathVariable String status) {
        return ResponseEntity.ok(orderService.findByStatus(status));
    }

    @GetMapping("/currency/{currency}")
    public ResponseEntity<List<OrderDocument>> getByCurrency(
            @PathVariable String currency) {
        return ResponseEntity.ok(orderService.findByCurrency(currency));
    }

    @PatchMapping("/{id}/status")
    public ResponseEntity<OrderDocument> updateStatus(
            @PathVariable String id,
            @RequestParam String status) {

        Optional<OrderDocument> existing = orderService.findById(id);
        String oldStatus = existing.map(OrderDocument::getStatus).orElse(null);

        Optional<OrderDocument> updated = orderService.updateStatus(id, status);

        updated.ifPresent(order ->
            auditService.log(
                id, "UPDATED",
                oldStatus, status,
                order.getAmount(), order.getCurrency(),
                "Status updated via API"
            )
        );

        return updated.map(ResponseEntity::ok)
                      .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/retry")
    public ResponseEntity<OrderResponse> retryOrder(@PathVariable String id) {

        Optional<OrderDocument> existing = orderService.findById(id);

        if (existing.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new OrderResponse(id, "Order not found: " + id));
        }

        OrderDocument order = existing.get();
        producer.sendOrder(
            order.getId(),
            order.getAmount(),
            order.getCurrency(),
            "CREATED"
        );

        auditService.log(
            id, "RETRIED",
            order.getStatus(), "CREATED",
            order.getAmount(), order.getCurrency(),
            "Order retried via API"
        );

        return ResponseEntity.ok(new OrderResponse(id, "Order retried: " + id));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<OrderResponse> deleteOrder(@PathVariable String id) {

        Optional<OrderDocument> existing = orderService.findById(id);

        if (existing.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new OrderResponse(id, "Order not found: " + id));
        }

        OrderDocument order = existing.get();
        auditService.log(
            id, "DELETED",
            order.getStatus(), null,
            order.getAmount(), order.getCurrency(),
            "Order deleted via API"
        );

        orderService.deleteById(id);
        return ResponseEntity.ok(new OrderResponse(id, "Order deleted: " + id));
    }

    @GetMapping("/count")
    public ResponseEntity<Map<String, Object>> getCount() {
        Map<String, Object> response = new HashMap<>();
        response.put("mongoTotal", orderService.count());
        response.put("auditTotal", auditService.count());
        return ResponseEntity.ok(response);
    }

    // -------------------------------------------------------
    // AUDIT ENDPOINTS - reads from PostgreSQL
    // -------------------------------------------------------

    @GetMapping("/{id}/history")
    public ResponseEntity<Map<String, Object>> getOrderHistory(
            @PathVariable String id) {

        List<OrderAuditLog> history = auditService.getHistory(id);

        Map<String, Object> response = new HashMap<>();
        response.put("orderId",     id);
        response.put("history",     history);
        response.put("totalEvents", history.size());

        return ResponseEntity.ok(response);
    }

    // -------------------------------------------------------
    // REPORT ENDPOINTS - SQL queries on PostgreSQL
    // -------------------------------------------------------

    @GetMapping("/reports/revenue")
    public ResponseEntity<Map<String, Object>> getRevenueReport() {
        Map<String, Object> response = new HashMap<>();
        response.put("report", "Revenue by Currency");
        response.put("data",   auditService.getRevenueReport());
        return ResponseEntity.ok(response);
    }

    @GetMapping("/reports/daily")
    public ResponseEntity<Map<String, Object>> getDailyReport() {
        Map<String, Object> response = new HashMap<>();
        response.put("report", "Daily Orders");
        response.put("data",   auditService.getDailyReport());
        return ResponseEntity.ok(response);
    }

    @GetMapping("/reports/top-orders")
    public ResponseEntity<Map<String, Object>> getTopOrders(
            @RequestParam(defaultValue = "5") int limit) {
        Map<String, Object> response = new HashMap<>();
        response.put("report", "Top " + limit + " Orders by Amount");
        response.put("data",   auditService.getTopOrders(limit));
        return ResponseEntity.ok(response);
    }

    // -------------------------------------------------------
    // CLEANUP ENDPOINT
    // -------------------------------------------------------

    @DeleteMapping("/cleanup/test-data")
    public ResponseEntity<Map<String, Object>> cleanupTestData() {

        long mongoDeleted = orderService.deleteTestData();
        long auditDeleted = auditService.cleanupTestData();

        Map<String, Object> response = new HashMap<>();
        response.put("message",      "Test data cleaned successfully");
        response.put("mongoDeleted", mongoDeleted);
        response.put("auditDeleted", auditDeleted);

        return ResponseEntity.ok(response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<OrderResponse> handleException(Exception ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
            .body(new OrderResponse("error", ex.getMessage()));
    }
}