package com.shop;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public PaymentPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    void publishCharged(String orderId, double total) {
        kafkaTemplate.send("payments.charged", orderId + ":" + total);
    }

    void publishReconciled() {
        kafkaTemplate.send("payments.reconciled", "ok");
    }
}
