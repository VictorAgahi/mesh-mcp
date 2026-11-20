package com.shop;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentsKafkaListener {

    private final FulfilmentService fulfilment;

    public PaymentsKafkaListener(FulfilmentService fulfilment) {
        this.fulfilment = fulfilment;
    }

    @KafkaListener(topics = "payments.charged", groupId = "fulfilment")
    public void onCharged(String message) {
        fulfilment.start(message.split(":")[0]);
    }
}
