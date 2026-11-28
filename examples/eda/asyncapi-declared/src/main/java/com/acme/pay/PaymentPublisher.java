package com.acme.pay;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    void captured(String orderId, long amount) {
        kafkaTemplate.send("payments.captured", new PaymentEvent(orderId, amount));
    }
}
