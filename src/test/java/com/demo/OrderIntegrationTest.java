package com.demo;

import com.demo.avro.Order;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    topics = {"orders"},
    brokerProperties = {
        "listeners=PLAINTEXT://localhost:9093",
        "port=9093"
    }
)
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "spring.kafka.properties.schema.registry.url=mock://test-registry",
    "spring.kafka.consumer.auto-offset-reset=earliest"
})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OrderIntegrationTest {

    @Autowired
    private OrderProducer orderProducer;

    @Autowired
    private TestKafkaListener testKafkaListener;

    @BeforeEach
    void setUp() {
        testKafkaListener.clear();
    }

    @TestConfiguration
    static class TestConfig {
        @Bean
        public TestKafkaListener testKafkaListener() {
            return new TestKafkaListener();
        }
    }

    static class TestKafkaListener {

        private final BlockingQueue<Order> receivedOrders = new LinkedBlockingQueue<>();

        @KafkaListener(
            topics = "orders",
            groupId = "integration-test-group",
            containerFactory = "kafkaListenerContainerFactory"
        )
        public void listen(Order order) {
            System.out.println("🎯 Test listener received: " + order.getId());
            receivedOrders.add(order);
        }

        public Order poll(long timeout, TimeUnit unit) throws InterruptedException {
            return receivedOrders.poll(timeout, unit);
        }

        public void clear() {
            receivedOrders.clear();
        }
    }

    @Test
    @org.junit.jupiter.api.Order(1)
    void shouldSendAndReceiveOrder() throws InterruptedException {

        orderProducer.sendOrder("TEST-001", 99.99, "USD");

        Order received = testKafkaListener.poll(10, TimeUnit.SECONDS);

        assertThat(received).isNotNull();
        assertThat(received.getId().toString()).isEqualTo("TEST-001");
        assertThat(received.getAmount()).isEqualTo(99.99);
        assertThat(received.getCurrency().toString()).isEqualTo("USD");

        System.out.println("✅ Test 1 passed!");
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    void shouldSendMultipleOrdersAndReceiveAll() throws InterruptedException {

        orderProducer.sendOrder("ORDER-A", 100.0, "USD");
        orderProducer.sendOrder("ORDER-B", 200.0, "EUR");
        orderProducer.sendOrder("ORDER-C", 300.0, "GBP");

        Order first  = testKafkaListener.poll(10, TimeUnit.SECONDS);
        Order second = testKafkaListener.poll(10, TimeUnit.SECONDS);
        Order third  = testKafkaListener.poll(10, TimeUnit.SECONDS);

        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(third).isNotNull();

        System.out.println("✅ Test 2 passed!");
    }

    @Test
    @org.junit.jupiter.api.Order(3)
    void shouldUseDefaultStatusWhenNotProvided() throws InterruptedException {

        orderProducer.sendOrder("OLD-001", 50.0, "USD");

        Order received = testKafkaListener.poll(10, TimeUnit.SECONDS);

        assertThat(received).isNotNull();
        assertThat(received.getStatus().toString()).isEqualTo("CREATED");

        System.out.println("✅ Test 3 passed! Default status = "
            + received.getStatus());
    }

    @Test
    @org.junit.jupiter.api.Order(4)
    void shouldSendOrderWithExplicitStatus() throws InterruptedException {

        orderProducer.sendOrder("NEW-001", 150.0, "USD", "CREATED");
        orderProducer.sendOrder("NEW-002", 250.0, "EUR", "PROCESSED");
        orderProducer.sendOrder("NEW-003", 350.0, "GBP", "FAILED");

        Order created   = testKafkaListener.poll(10, TimeUnit.SECONDS);
        Order processed = testKafkaListener.poll(10, TimeUnit.SECONDS);
        Order failed    = testKafkaListener.poll(10, TimeUnit.SECONDS);

        assertThat(created.getStatus().toString()).isEqualTo("CREATED");
        assertThat(processed.getStatus().toString()).isEqualTo("PROCESSED");
        assertThat(failed.getStatus().toString()).isEqualTo("FAILED");

        System.out.println("✅ Test 4 passed!");
        System.out.println("   " + created.getId()   + " → " + created.getStatus());
        System.out.println("   " + processed.getId() + " → " + processed.getStatus());
        System.out.println("   " + failed.getId()    + " → " + failed.getStatus());
    }

    @Test
    @org.junit.jupiter.api.Order(5)
    void shouldHandleAllOrderLifecycleStatuses() throws InterruptedException {

        String[] statuses = {"CREATED", "VALIDATED", "PROCESSED", "SHIPPED", "DELIVERED"};

        for (int i = 0; i < statuses.length; i++) {
            orderProducer.sendOrder("LIFECYCLE-00" + i, 100.0, "USD", statuses[i]);
        }

        System.out.println("✅ Test 5 - Order lifecycle:");
        for (int i = 0; i < statuses.length; i++) {
            Order received = testKafkaListener.poll(15, TimeUnit.SECONDS);
            assertThat(received)
                .as("Expected message for status: " + statuses[i])
                .isNotNull();
            System.out.println("   Step " + (i + 1) + ": "
                + received.getId() + " → " + received.getStatus());
        }

        System.out.println("✅ All lifecycle statuses received!");
    }
}