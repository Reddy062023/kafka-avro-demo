package com.demo;

import com.pos.avro.Transaction;
import com.pos.avro.PaymentEvent;
import com.pos.avro.LoyaltyEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PosTransactionProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PosTransactionProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // ── Send a completed POS transaction ──────────────────────────
    public void sendTransaction(String transactionId, String storeId,
                                String registerId, double totalAmount,
                                double taxAmount, String paymentMethod,
                                String cashierId) {
        Transaction txn = new Transaction();
        txn.setTransactionId(transactionId);
        txn.setStoreId(storeId);
        txn.setRegisterId(registerId);
        txn.setTotalAmount(totalAmount);
        txn.setTaxAmount(taxAmount);
        txn.setPaymentMethod(paymentMethod);
        txn.setStatus("COMPLETED");
        txn.setCashierId(cashierId);
        txn.setTimestamp(System.currentTimeMillis());

        kafkaTemplate.send("pos-transactions", transactionId, txn);
        System.out.println("✅ POS Transaction sent: " + transactionId);
    }

    // ── Send a payment event (card payment) ───────────────────────
    public void sendPaymentEvent(String transactionId, String cardType,
                                 String maskedPAN, double amount,
                                 String authCode, boolean approved,
                                 String failureReason, double cashBackAmount) {
        PaymentEvent payment = new PaymentEvent();
        payment.setTransactionId(transactionId);
        payment.setCardType(cardType);
        payment.setMaskedPAN(maskedPAN);
        payment.setAmount(amount);
        payment.setAuthCode(authCode);
        payment.setApproved(approved);
        payment.setFailureReason(failureReason);
        payment.setCashBackAmount(cashBackAmount);
        payment.setTimestamp(System.currentTimeMillis());

        // approved → pos-payment-events
        // declined → goes to pos-payment-dlq via consumer error handler
        kafkaTemplate.send("pos-payment-events", transactionId, payment);
        System.out.println("✅ Payment event sent: " + transactionId
            + " approved=" + approved);
    }

    // ── Send a loyalty event ──────────────────────────────────────
    public void sendLoyaltyEvent(String transactionId, String loyaltyId,
                                 String eventType, long pointsBefore,
                                 long pointsEarned, long pointsRedeemed) {
        LoyaltyEvent loyalty = new LoyaltyEvent();
        loyalty.setTransactionId(transactionId);
        loyalty.setLoyaltyId(loyaltyId);
        loyalty.setEventType(eventType);
        loyalty.setPointsBefore(pointsBefore);
        loyalty.setPointsEarned(pointsEarned);
        loyalty.setPointsRedeemed(pointsRedeemed);
        loyalty.setPointsAfter(pointsBefore + pointsEarned - pointsRedeemed);
        loyalty.setTimestamp(System.currentTimeMillis());

        kafkaTemplate.send("pos-loyalty-events", loyaltyId, loyalty);
        System.out.println("✅ Loyalty event sent: " + loyaltyId
            + " earned=" + pointsEarned);
    }
}