package com.demo;

public class OrderResponse {

    private String id;
    private double amount;
    private String currency;
    private String status;
    private String message;
    private long timestamp;

    // Constructor for success
    public OrderResponse(String id, double amount,
                         String currency, String status) {
        this.id        = id;
        this.amount    = amount;
        this.currency  = currency;
        this.status    = status;
        this.message   = "Order processed successfully";
        this.timestamp = System.currentTimeMillis();
    }

    // Constructor for simple message
    public OrderResponse(String id, String message) {
        this.id        = id;
        this.message   = message;
        this.timestamp = System.currentTimeMillis();
    }

    public String getId()          { return id; }
    public double getAmount()      { return amount; }
    public String getCurrency()    { return currency; }
    public String getStatus()      { return status; }
    public String getMessage()     { return message; }
    public long getTimestamp()     { return timestamp; }
}