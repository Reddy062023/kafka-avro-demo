package com.demo.transaction;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface TransactionRepository
        extends MongoRepository<SalesTransaction, String> {

    List<SalesTransaction> findByStatus(String status);
    List<SalesTransaction> findByType(String type);
    List<SalesTransaction> findByTypeAndStatus(String type, String status);
}