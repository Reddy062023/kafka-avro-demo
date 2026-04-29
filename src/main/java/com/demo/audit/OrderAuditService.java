package com.demo.audit;

import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class OrderAuditService {

    private final OrderAuditRepository auditRepository;

    public OrderAuditService(OrderAuditRepository auditRepository) {
        this.auditRepository = auditRepository;
    }

    public void log(String orderId, String eventType,
                    String oldStatus, String newStatus,
                    double amount, String currency, String message) {

        OrderAuditLog entry = new OrderAuditLog(
            orderId, eventType, oldStatus,
            newStatus, amount, currency, message
        );

        auditRepository.save(entry);
        System.out.println("📋 Audit logged: " + orderId
            + " [" + eventType + "] " + oldStatus + " --> " + newStatus);
    }

    public List<OrderAuditLog> getHistory(String orderId) {
        return auditRepository.findByOrderIdOrderByCreatedAtAsc(orderId);
    }

    public List<Map<String, Object>> getRevenueReport() {
        List<Object[]> raw = auditRepository.revenuePerCurrency();
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object[] row : raw) {
            Map<String, Object> item = new HashMap<>();
            item.put("currency",     row[0]);
            item.put("totalRevenue", row[1]);
            item.put("orderCount",   row[2]);
            result.add(item);
        }
        return result;
    }

    public List<Map<String, Object>> getDailyReport() {
        List<Object[]> raw = auditRepository.dailyReport();
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object[] row : raw) {
            Map<String, Object> item = new HashMap<>();
            item.put("day",         row[0]);
            item.put("orderCount",  row[1]);
            item.put("totalAmount", row[2]);
            result.add(item);
        }
        return result;
    }

    public List<OrderAuditLog> getTopOrders(int limit) {
        return auditRepository.topOrdersByAmount(PageRequest.of(0, limit));
    }

    public long count() {
        return auditRepository.count();
    }

    public long cleanupTestData() {
        List<OrderAuditLog> testData = auditRepository.findAll()
            .stream()
            .filter(a -> a.getOrderId().startsWith("KARATE-")
                      || a.getOrderId().startsWith("TEST-")
                      || a.getOrderId().startsWith("DEV-")
                      || a.getOrderId().startsWith("BAD-"))
            .collect(Collectors.toList());

        auditRepository.deleteAll(testData);
        System.out.println("Cleaned " + testData.size() + " audit entries");
        return testData.size();
    }
}