package com.demo;

import com.intuit.karate.junit5.Karate;
import org.junit.jupiter.api.extension.ExtendWith;
import io.qameta.allure.junit5.AllureJunit5;

@ExtendWith(AllureJunit5.class)
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