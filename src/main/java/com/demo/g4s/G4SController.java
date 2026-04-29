package com.demo.g4s;

import com.demo.transaction.SalesTransaction;
import com.demo.transaction.TransactionRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/g4s")
public class G4SController {

    private final TransactionRepository repository;

    public G4SController(TransactionRepository repository) {
        this.repository = repository;
    }

    // GET /g4s/transactions/{id} — check if transaction is SAFE
    @GetMapping("/transactions/{id}")
    public ResponseEntity<SalesTransaction> getTransaction(
            @PathVariable String id) {
        return repository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // GET /g4s/transactions — all SAFE transactions
    @GetMapping("/transactions")
    public ResponseEntity<List<SalesTransaction>> getSafeTransactions() {
        return ResponseEntity.ok(
            repository.findByStatus("SAFE"));
    }

    // GET /g4s/summary — count by status
    @GetMapping("/summary")
    public ResponseEntity<Map<String, Object>> getSummary() {
        Map<String, Object> summary = new HashMap<>();
        summary.put("SAFE",         repository.findByStatus("SAFE").size());
        summary.put("PENDING_SAFE", repository.findByStatus("PENDING_SAFE").size());
        summary.put("PROCESSED",    repository.findByStatus("PROCESSED").size());
        summary.put("CREATED",      repository.findByStatus("CREATED").size());
        summary.put("total",        repository.findAll().size());
        return ResponseEntity.ok(summary);
    }
}