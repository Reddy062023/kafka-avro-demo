package com.demo.g4s;

import com.demo.transaction.SalesTransaction;
import com.demo.transaction.TransactionRepository;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class G4SManagerService {

    private final TransactionRepository repository;

    public G4SManagerService(TransactionRepository repository) {
        this.repository = repository;
    }

    @RabbitListener(queues = "cash.queue")
    public void processCashTransaction(SalesTransaction txn) {

        System.out.println("G4S received CASH transaction: "
            + txn.getTransactionId()
            + " amount=" + txn.getAmount());

        // Idempotency check — do not process twice
        var existing = repository.findById(txn.getTransactionId());
        if (existing.isPresent()
                && "SAFE".equals(existing.get().getStatus())) {
            System.out.println("Already SAFE, skipping: "
                + txn.getTransactionId());
            return;
        }

        // Update to SAFE
        txn.setStatus("SAFE");
        repository.save(txn);

        System.out.println("SAFE confirmed: " + txn.getTransactionId());
    }
}