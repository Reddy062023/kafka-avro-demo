package com.demo;

import com.demo.avro.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
public class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private OrderProducer orderProducer;

    @Autowired
    private OrderStore orderStore;

    @BeforeEach
    void setUp() {
        // Pre-load an order into store for GET tests
        Order existing = new Order();
        existing.setId("EXISTING-001");
        existing.setAmount(100.0);
        existing.setCurrency("USD");
        existing.setStatus("CREATED");
        orderStore.save(existing);
    }

    @Test
    void shouldSendOrderViaPostJson() throws Exception {

        OrderRequest request = new OrderRequest();
        request.setId("API-001");
        request.setAmount(250.0);
        request.setCurrency("USD");
        request.setStatus("CREATED");

        mockMvc.perform(post("/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.id").value("API-001"))
            .andExpect(jsonPath("$.amount").value(250.0))
            .andExpect(jsonPath("$.currency").value("USD"))
            .andExpect(jsonPath("$.status").value("CREATED"));

        verify(orderProducer, times(1))
            .sendOrder("API-001", 250.0, "USD", "CREATED");

        System.out.println("✅ POST /orders works!");
    }

    @Test
    void shouldGetExistingOrder() throws Exception {

        mockMvc.perform(get("/orders/EXISTING-001"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").value("EXISTING-001"))
            .andExpect(jsonPath("$.amount").value(100.0))
            .andExpect(jsonPath("$.status").value("CREATED"));

        System.out.println("✅ GET /orders/{id} works!");
    }

    @Test
    void shouldReturn404ForUnknownOrder() throws Exception {

        mockMvc.perform(get("/orders/UNKNOWN-999"))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.message")
                .value("Order not found: UNKNOWN-999"));

        System.out.println("✅ GET /orders/{id} returns 404 correctly!");
    }

    @Test
    void shouldRetryExistingOrder() throws Exception {

        mockMvc.perform(post("/orders/EXISTING-001/retry"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.message")
                .value("Order retried: EXISTING-001"));

        verify(orderProducer, times(1))
            .sendOrder("EXISTING-001", 100.0, "USD", "CREATED");

        System.out.println("✅ POST /orders/{id}/retry works!");
    }

    @Test
    void shouldReturn404WhenRetryingUnknownOrder() throws Exception {

        mockMvc.perform(post("/orders/UNKNOWN-999/retry"))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.message")
                .value("Order not found: UNKNOWN-999"));

        System.out.println("✅ Retry returns 404 for unknown order!");
    }

    @Test
    void shouldGetAllOrders() throws Exception {

        mockMvc.perform(get("/orders"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.total").isNumber());

        System.out.println("✅ GET /orders returns all orders!");
    }
}