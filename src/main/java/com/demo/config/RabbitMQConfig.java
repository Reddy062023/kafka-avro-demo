package com.demo.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    // ── MAIN CASH QUEUE ──────────────────────────────────────────────
    @Bean
    public Queue cashQueue() {
        return QueueBuilder.durable("cash.queue")
            .withArgument("x-dead-letter-exchange", "cash.dlq.exchange")
            .build();
    }

    @Bean
    public DirectExchange cashExchange() {
        return new DirectExchange("cash.exchange");
    }

    @Bean
    public Binding cashBinding(Queue cashQueue,
                               DirectExchange cashExchange) {
        return BindingBuilder
            .bind(cashQueue)
            .to(cashExchange)
            .with("cash.routing.key");
    }

    // ── DEAD LETTER QUEUE ────────────────────────────────────────────
    @Bean
    public Queue cashDlq() {
        return QueueBuilder.durable("cash.dlq").build();
    }

    @Bean
    public DirectExchange cashDlqExchange() {
        return new DirectExchange("cash.dlq.exchange");
    }

    @Bean
    public Binding cashDlqBinding() {
        return BindingBuilder
            .bind(cashDlq())
            .to(cashDlqExchange())
            .with("cash.queue");
    }

    // ── JSON CONVERTER with Java 8 date support ───────────────────────
    @Bean
    public Jackson2JsonMessageConverter messageConverter() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return new Jackson2JsonMessageConverter(mapper);
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter());
        return template;
    }
}