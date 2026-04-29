package com.demo.transaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/test")
public class TransactionController {

    private final KafkaTemplate<String, String> stringKafkaTemplate;
    private final ObjectMapper                  objectMapper;
    private final TransactionRepository         repository;

    public TransactionController(KafkaTemplate<String, String> stringKafkaTemplate,
                                 ObjectMapper objectMapper,
                                 TransactionRepository repository) {
        this.stringKafkaTemplate = stringKafkaTemplate;
        this.objectMapper        = objectMapper;
        this.repository          = repository;
    }

    // POST /test/kafka/publish/sales-transactions
    // This is our HTTP bridge — Postman calls this to send events to Kafka
    @PostMapping("/kafka/publish/{topic}")
    public ResponseEntity<Map<String, Object>> publishToKafka(
            @PathVariable String topic,
            @RequestBody SalesTransaction txn) throws Exception {

        // Validate
        if (txn.getTransactionId() == null || txn.getTransactionId().isBlank()) {
            Map<String, Object> err = new HashMap<>();
            err.put("error", "transactionId is required");
            return ResponseEntity.badRequest().body(err);
        }

        if (txn.getAmount() < 0) {
            Map<String, Object> err = new HashMap<>();
            err.put("error", "amount must be >= 0");
            return ResponseEntity.badRequest().body(err);
        }

        // Set defaults
        if (txn.getCurrency() == null) txn.setCurrency("USD");
        txn.setStatus("CREATED");

        // Save initial state to MongoDB
        repository.save(txn);

        // Publish to Kafka as JSON string
        String payload = objectMapper.writeValueAsString(txn);
        stringKafkaTemplate.send(topic, txn.getTransactionId(), payload);

        System.out.println("Published to Kafka topic [" + topic + "]: "
            + txn.getTransactionId() + " type=" + txn.getType());

        Map<String, Object> response = new HashMap<>();
        response.put("message",       "Published to Kafka topic: " + topic);
        response.put("transactionId", txn.getTransactionId());
        response.put("type",          txn.getType());
        response.put("amount",        txn.getAmount());
        response.put("currency",      txn.getCurrency());
        response.put("kafkaTopic",    topic);
        response.put("status",        "CREATED");

        return ResponseEntity.ok(response);
    }

    // GET /test/kafka/transactions/{id}
    @GetMapping("/kafka/transactions/{id}")
    public ResponseEntity<SalesTransaction> getTransaction(
            @PathVariable String id) {
        return repository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // GET /test/kafka/transactions
    @GetMapping("/kafka/transactions")
    public ResponseEntity<List<SalesTransaction>> getAllTransactions() {
        return ResponseEntity.ok(repository.findAll());
    }

    // DELETE /test/kafka/cleanup
    @DeleteMapping("/kafka/cleanup")
    public ResponseEntity<Map<String, Object>> cleanup() {
        List<SalesTransaction> testData = repository.findAll()
                .stream()
                .filter(t -> t.getTransactionId().startsWith("KARATE-")
                          || t.getTransactionId().startsWith("TEST-")
                          || t.getTransactionId().startsWith("POSTMAN-")
                          || t.getTransactionId().startsWith("TC"))
                .toList();

        repository.deleteAll(testData);

        Map<String, Object> response = new HashMap<>();
        response.put("message", "Cleanup complete");
        response.put("deleted", testData.size());
        return ResponseEntity.ok(response);
    }
}