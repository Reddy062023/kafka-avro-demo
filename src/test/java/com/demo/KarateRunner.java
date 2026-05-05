package com.demo;
import com.intuit.karate.junit5.Karate;
class KarateRunner {
    @Karate.Test
    Karate testPosCashTransaction() {
        return Karate.run("classpath:karate/pos/pos-cash-transaction.feature");
    }
    @Karate.Test
    Karate testPosCardPayment() {
        return Karate.run("classpath:karate/pos/pos-card-payment.feature");
    }
}