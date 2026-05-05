package com.demo;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.Map;
import java.util.HashMap;

@RestController
@RequestMapping("/pos")
public class PosController {

    private final PosTransactionProducer producer;

    public PosController(PosTransactionProducer producer) {
        this.producer = producer;
    }

    // POST /pos/transactions — cash or card sale
    @PostMapping("/transactions")
    public ResponseEntity<Map<String, Object>> createTransaction(
            @RequestBody Map<String, Object> request) {

        String txnId       = (String) request.get("transactionId");
        String storeId     = (String) request.getOrDefault("storeId", "STORE-001");
        String registerId  = (String) request.getOrDefault("registerId", "REG-001");
        double totalAmount = ((Number) request.get("totalAmount")).doubleValue();
        double taxAmount   = request.containsKey("taxAmount")
                             ? ((Number) request.get("taxAmount")).doubleValue() : 0.0;
        String payMethod   = (String) request.getOrDefault("paymentMethod", "CASH");
        String cashierId   = (String) request.getOrDefault("cashierId", "C001");

        // Validate
        if (txnId == null || txnId.isEmpty()) {
            return ResponseEntity.badRequest()
                .body(Map.of("error", "transactionId is required"));
        }

        // Send to Kafka
        producer.sendTransaction(txnId, storeId, registerId,
            totalAmount, taxAmount, payMethod, cashierId);

        Map<String, Object> response = new HashMap<>();
        response.put("transactionId", txnId);
        response.put("storeId",       storeId);
        response.put("totalAmount",   totalAmount);
        response.put("taxAmount",     taxAmount);
        response.put("paymentMethod", payMethod);
        response.put("status",        "COMPLETED");
        response.put("message",       "Transaction processed successfully");
        response.put("timestamp",     System.currentTimeMillis());

        return ResponseEntity.status(201).body(response);
    }

    // POST /pos/payments — card payment event
    @PostMapping("/payments")
    public ResponseEntity<Map<String, Object>> processPayment(
            @RequestBody Map<String, Object> request) {

        String txnId        = (String) request.get("transactionId");
        String cardType     = (String) request.getOrDefault("cardType", "VISA");
        String maskedPAN    = (String) request.getOrDefault("maskedPAN", "****1234");
        double amount       = ((Number) request.get("amount")).doubleValue();
        String authCode     = (String) request.getOrDefault("authCode", "AUTH123");
        boolean approved    = (boolean) request.getOrDefault("approved", true);
        String failReason   = (String) request.getOrDefault("failureReason", "");
        double cashBack     = request.containsKey("cashBackAmount")
                             ? ((Number) request.get("cashBackAmount")).doubleValue() : 0.0;

        producer.sendPaymentEvent(txnId, cardType, maskedPAN, amount,
            authCode, approved, failReason, cashBack);

        Map<String, Object> response = new HashMap<>();
        response.put("transactionId", txnId);
        response.put("cardType",      cardType);
        response.put("amount",        amount);
        response.put("approved",      approved);
        response.put("authCode",      approved ? authCode : "");
        response.put("failureReason", failReason);
        response.put("timestamp",     System.currentTimeMillis());

        return ResponseEntity.status(approved ? 201 : 402).body(response);
    }

    // POST /pos/loyalty — loyalty points event
    @PostMapping("/loyalty")
    public ResponseEntity<Map<String, Object>> processLoyalty(
            @RequestBody Map<String, Object> request) {

        String txnId      = (String) request.get("transactionId");
        String loyaltyId  = (String) request.get("loyaltyId");
        String eventType  = (String) request.getOrDefault("eventType", "EARN");
        long before       = ((Number) request.getOrDefault("pointsBefore", 0)).longValue();
        long earned       = ((Number) request.getOrDefault("pointsEarned", 0)).longValue();
        long redeemed     = ((Number) request.getOrDefault("pointsRedeemed", 0)).longValue();

        producer.sendLoyaltyEvent(txnId, loyaltyId, eventType,
            before, earned, redeemed);

        Map<String, Object> response = new HashMap<>();
        response.put("transactionId", txnId);
        response.put("loyaltyId",     loyaltyId);
        response.put("eventType",     eventType);
        response.put("pointsBefore",  before);
        response.put("pointsEarned",  earned);
        response.put("pointsRedeemed",redeemed);
        response.put("pointsAfter",   before + earned - redeemed);
        response.put("timestamp",     System.currentTimeMillis());

        return ResponseEntity.status(201).body(response);
    }
}