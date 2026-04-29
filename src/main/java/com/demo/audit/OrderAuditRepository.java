package com.demo.audit;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OrderAuditRepository extends JpaRepository<OrderAuditLog, Long> {

    List<OrderAuditLog> findByOrderIdOrderByCreatedAtAsc(String orderId);

    List<OrderAuditLog> findByEventType(String eventType);

    @Query("SELECT a.currency, SUM(a.amount), COUNT(a) " +
           "FROM OrderAuditLog a " +
           "WHERE a.eventType = 'CREATED' " +
           "GROUP BY a.currency " +
           "ORDER BY SUM(a.amount) DESC")
    List<Object[]> revenuePerCurrency();

    @Query(value = "SELECT DATE(created_at), COUNT(*), SUM(amount) " +
                   "FROM order_audit_log " +
                   "WHERE event_type = 'CREATED' " +
                   "GROUP BY DATE(created_at) " +
                   "ORDER BY DATE(created_at) DESC",
           nativeQuery = true)
    List<Object[]> dailyReport();

    @Query("SELECT a FROM OrderAuditLog a " +
           "WHERE a.eventType = 'CREATED' " +
           "ORDER BY a.amount DESC")
    List<OrderAuditLog> topOrdersByAmount(Pageable pageable);
}