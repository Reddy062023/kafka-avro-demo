package com.demo;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OrderRepository extends MongoRepository<OrderDocument, String> {

    // Spring Data generates these automatically — no implementation needed!

    // Find all orders by status
    List<OrderDocument> findByStatus(String status);

    // Find all orders by currency
    List<OrderDocument> findByCurrency(String currency);

    // Find orders with amount greater than a value
    List<OrderDocument> findByAmountGreaterThan(double amount);

    // Find orders by status and currency
    List<OrderDocument> findByStatusAndCurrency(String status, String currency);
}