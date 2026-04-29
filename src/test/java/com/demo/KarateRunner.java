package com.demo;

import com.intuit.karate.junit5.Karate;

class KarateRunner {

    @Karate.Test
    Karate testOrders() {
        return Karate.run("classpath:karate/orders/orders.feature")
                     .relativeTo(getClass());
    }
}