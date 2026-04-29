package com.demo;

import jakarta.validation.constraints.NotBlank;

public class OrderRequest {

    @NotBlank(message = "Order ID is required")
    private String id;

    private double amount;

    private String currency = "USD";
    private String status   = "CREATED";

    public String getId()                { return id; }
    public void setId(String id)         { this.id = id; }

    public double getAmount()            { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getCurrency()          { return currency; }
    public void setCurrency(String c)    { this.currency = c; }

    public String getStatus()            { return status; }
    public void setStatus(String s)      { this.status = s; }
}