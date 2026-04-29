package com.demo;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.LocalDateTime;

@Document(collection = "orders")
public class OrderDocument {

    @Id
    private String id;
    private double amount;
    private String currency;
    private String status;
    private LocalDateTime receivedAt;
    private LocalDateTime updatedAt;

    public OrderDocument() {}

    public OrderDocument(String id, double amount,
                         String currency, String status) {
        this.id         = id;
        this.amount     = amount;
        this.currency   = currency;
        this.status     = status;
        this.receivedAt = LocalDateTime.now();
        this.updatedAt  = LocalDateTime.now();
    }

    public String getId()                        { return id; }
    public void setId(String id)                 { this.id = id; }

    public double getAmount()                    { return amount; }
    public void setAmount(double amount)         { this.amount = amount; }

    public String getCurrency()                  { return currency; }
    public void setCurrency(String currency)     { this.currency = currency; }

    public String getStatus()                    { return status; }
    public void setStatus(String status)         { this.status = status; }

    public LocalDateTime getReceivedAt()                   { return receivedAt; }
    public void setReceivedAt(LocalDateTime receivedAt)    { this.receivedAt = receivedAt; }

    public LocalDateTime getUpdatedAt()                    { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt)      { this.updatedAt = updatedAt; }

    @Override
    public String toString() {
        return "OrderDocument{id='" + id + "', amount=" + amount +
               ", currency='" + currency + "', status='" + status + "'}";
    }
}