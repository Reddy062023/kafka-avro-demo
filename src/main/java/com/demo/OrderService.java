package com.demo;

import org.springframework.stereotype.Service;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class OrderService {

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    // Save a new order
    public OrderDocument save(OrderDocument order) {
        System.out.println("Saving to MongoDB: " + order.getId());
        return orderRepository.save(order);
    }

    // Find by ID
    public Optional<OrderDocument> findById(String id) {
        return orderRepository.findById(id);
    }

    // Find all orders
    public List<OrderDocument> findAll() {
        return orderRepository.findAll();
    }

    // Find by status
    public List<OrderDocument> findByStatus(String status) {
        return orderRepository.findByStatus(status);
    }

    // Find by currency
    public List<OrderDocument> findByCurrency(String currency) {
        return orderRepository.findByCurrency(currency);
    }

    // Update order status
    public Optional<OrderDocument> updateStatus(String id, String newStatus) {
        Optional<OrderDocument> existing = orderRepository.findById(id);
        if (existing.isPresent()) {
            OrderDocument order = existing.get();
            order.setStatus(newStatus);
            order.setUpdatedAt(LocalDateTime.now());
            orderRepository.save(order);
            System.out.println("Updated status: " + id + " to " + newStatus);
            return Optional.of(order);
        }
        return Optional.empty();
    }

    // Delete by ID
    public boolean deleteById(String id) {
        if (orderRepository.existsById(id)) {
            orderRepository.deleteById(id);
            System.out.println("Deleted order: " + id);
            return true;
        }
        return false;
    }

    // Count all orders
    public long count() {
        return orderRepository.count();
    }

    // Delete test data (KARATE-, TEST-, DEV-, BAD- prefixes)
    public long deleteTestData() {
        List<OrderDocument> testData = orderRepository.findAll()
            .stream()
            .filter(o -> o.getId().startsWith("KARATE-")
                      || o.getId().startsWith("TEST-")
                      || o.getId().startsWith("DEV-")
                      || o.getId().startsWith("BAD-"))
            .collect(Collectors.toList());

        orderRepository.deleteAll(testData);
        System.out.println("Cleaned " + testData.size() + " MongoDB orders");
        return testData.size();
    }
}