package com.demo;

import com.demo.avro.Order;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class OrderStore {

    private final Map<String, Order> store = new ConcurrentHashMap<>();

    public void save(Order order) {
        store.put(order.getId().toString(), order);
        System.out.println("💾 Stored order: " + order.getId());
    }

    public Order findById(String id) {
        return store.get(id);
    }

    public boolean exists(String id) {
        return store.containsKey(id);
    }

    public Map<String, Order> findAll() {
        return store;
    }
}