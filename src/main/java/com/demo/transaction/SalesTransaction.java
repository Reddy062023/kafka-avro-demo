package com.demo.transaction;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.LocalDateTime;

@Document(collection = "transactions")
public class SalesTransaction {

    @Id
    private String transactionId;
    private String type;         // CASH, CARD, GIFT_CERT, CHECK, STORE_CARD
    private double amount;
    private String currency;
    private String storeId;
    private String status;       // CREATED, PENDING_SAFE, PROCESSED, SAFE, FAILED
    private LocalDateTime receivedAt;
    private LocalDateTime updatedAt;

    public SalesTransaction() {}

    public SalesTransaction(String transactionId, String type,
                            double amount, String currency) {
        this.transactionId = transactionId;
        this.type          = type;
        this.amount        = amount;
        this.currency      = currency;
        this.status        = "CREATED";
        this.receivedAt    = LocalDateTime.now();
        this.updatedAt     = LocalDateTime.now();
    }

    public String getTransactionId()                    { return transactionId; }
    public void setTransactionId(String transactionId)  { this.transactionId = transactionId; }
    public String getType()                             { return type; }
    public void setType(String type)                    { this.type = type; }
    public double getAmount()                           { return amount; }
    public void setAmount(double amount)                { this.amount = amount; }
    public String getCurrency()                         { return currency; }
    public void setCurrency(String currency)            { this.currency = currency; }
    public String getStoreId()                          { return storeId; }
    public void setStoreId(String storeId)              { this.storeId = storeId; }
    public String getStatus()                           { return status; }
    public void setStatus(String status)                { this.status = status; this.updatedAt = LocalDateTime.now(); }
    public LocalDateTime getReceivedAt()                { return receivedAt; }
    public void setReceivedAt(LocalDateTime receivedAt) { this.receivedAt = receivedAt; }
    public LocalDateTime getUpdatedAt()                 { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt)   { this.updatedAt = updatedAt; }
}