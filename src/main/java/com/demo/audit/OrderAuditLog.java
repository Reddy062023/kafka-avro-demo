package com.demo.audit;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "order_audit_log")
public class OrderAuditLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "old_status")
    private String oldStatus;

    @Column(name = "new_status")
    private String newStatus;

    @Column(name = "amount")
    private double amount;

    @Column(name = "currency")
    private String currency;

    @Column(name = "message", length = 500)
    private String message;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    public OrderAuditLog() {}

    public OrderAuditLog(String orderId, String eventType,
                         String oldStatus, String newStatus,
                         double amount, String currency, String message) {
        this.orderId   = orderId;
        this.eventType = eventType;
        this.oldStatus = oldStatus;
        this.newStatus = newStatus;
        this.amount    = amount;
        this.currency  = currency;
        this.message   = message;
        this.createdAt = LocalDateTime.now();
    }

    public Long getId()                              { return id; }
    public void setId(Long id)                       { this.id = id; }

    public String getOrderId()                       { return orderId; }
    public void setOrderId(String orderId)           { this.orderId = orderId; }

    public String getEventType()                     { return eventType; }
    public void setEventType(String eventType)       { this.eventType = eventType; }

    public String getOldStatus()                     { return oldStatus; }
    public void setOldStatus(String oldStatus)       { this.oldStatus = oldStatus; }

    public String getNewStatus()                     { return newStatus; }
    public void setNewStatus(String newStatus)       { this.newStatus = newStatus; }

    public double getAmount()                        { return amount; }
    public void setAmount(double amount)             { this.amount = amount; }

    public String getCurrency()                      { return currency; }
    public void setCurrency(String currency)         { this.currency = currency; }

    public String getMessage()                       { return message; }
    public void setMessage(String message)           { this.message = message; }

    public LocalDateTime getCreatedAt()              { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt){ this.createdAt = createdAt; }
}