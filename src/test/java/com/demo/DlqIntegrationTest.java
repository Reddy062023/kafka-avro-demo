package com.demo;

import com.demo.avro.Order;
import org.junit.jupiter.api.BeforeEach;
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
    topics = {"orders", "orders-dlq"},
    brokerProperties = {
        "listeners=PLAINTEXT://localhost:9094",
        "port=9094"
    }
)
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "spring.kafka.properties.schema.registry.url=mock://test-registry",
    "spring.kafka.consumer.auto-offset-reset=earliest"
})
public class DlqIntegrationTest {

    @Autowired
    private OrderProducer orderProducer;

    @Autowired
    private TestDlqListener testDlqListener;

    @Autowired
    private TestOrderListener testOrderListener;

    @BeforeEach
    void setUp() {
        testDlqListener.clear();
        testOrderListener.clear();
    }

    @TestConfiguration
    static class TestConfig {

        @Bean
        public TestDlqListener testDlqListener() {
            return new TestDlqListener();
        }

        @Bean
        public TestOrderListener testOrderListener() {
            return new TestOrderListener();
        }
    }

    static class TestDlqListener {

        private final BlockingQueue<Object> dlqMessages = new LinkedBlockingQueue<>();

        @KafkaListener(topics = "orders-dlq", groupId = "test-dlq-group")
        public void listen(Object message) {
            System.out.println("🧪 Test DLQ received: " + message);
            dlqMessages.add(message != null ? message : "null-message");
        }

        public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
            return dlqMessages.poll(timeout, unit);
        }

        public void clear() { dlqMessages.clear(); }
    }

    static class TestOrderListener {

        private final BlockingQueue<Order> orders = new LinkedBlockingQueue<>();

        @KafkaListener(
            topics = "orders",
            groupId = "test-order-group",
            containerFactory = "kafkaListenerContainerFactory"
        )
        public void listen(Order order) {
            System.out.println("🧪 Test order received: " + order.getId());
            orders.add(order);
        }

        public Order poll(long timeout, TimeUnit unit) throws InterruptedException {
            return orders.poll(timeout, unit);
        }

        public void clear() { orders.clear(); }
    }

    @Test
    void shouldProcessValidOrderSuccessfully() throws InterruptedException {

        // WHEN
        orderProducer.sendOrder("VALID-001", 150.0, "USD", "CREATED");

        // THEN - received by order listener
        Order received = testOrderListener.poll(10, TimeUnit.SECONDS);
        assertThat(received).isNotNull();
        assertThat(received.getId().toString()).isEqualTo("VALID-001");

        // AND - nothing in DLQ
        Object dlqMessage = testDlqListener.poll(3, TimeUnit.SECONDS);
        assertThat(dlqMessage).isNull();

        System.out.println("✅ Valid order processed, DLQ empty!");
    }

    @Test
    void shouldSendInvalidOrderToDlq() throws InterruptedException {

        // WHEN - negative amount triggers exception in OrderConsumer
        orderProducer.sendOrder("BAD-001", -99.99, "USD", "CREATED");

        // THEN - lands in DLQ after retry
        Object dlqMessage = testDlqListener.poll(15, TimeUnit.SECONDS);
        assertThat(dlqMessage)
            .as("Expected bad message to land in DLQ")
            .isNotNull();

        System.out.println("✅ Bad order landed in DLQ: " + dlqMessage);
    }

    @Test
    void shouldNotAffectValidOrdersAfterBadMessage() throws InterruptedException {

        // WHEN - bad then good
        orderProducer.sendOrder("BAD-002",  -50.0, "USD", "CREATED");
        orderProducer.sendOrder("GOOD-002", 200.0, "USD", "CREATED");

        // THEN - bad goes to DLQ after retry
        Object dlqMessage = testDlqListener.poll(15, TimeUnit.SECONDS);
        assertThat(dlqMessage).isNotNull();
        System.out.println("✅ Bad message in DLQ: " + dlqMessage);

        // AND - good order is found in order listener
        // (drain up to 2 since BAD-002 may appear before error handler kicks in)
        Order first  = testOrderListener.poll(10, TimeUnit.SECONDS);
        Order second = testOrderListener.poll(5,  TimeUnit.SECONDS);

        boolean goodOrderFound = false;
        for (Order o : new Order[]{first, second}) {
            if (o != null && "GOOD-002".equals(o.getId().toString())) {
                goodOrderFound = true;
                System.out.println("✅ Good order found: " + o.getId());
            }
        }

        assertThat(goodOrderFound)
            .as("Expected GOOD-002 to be processed")
            .isTrue();
    }
}