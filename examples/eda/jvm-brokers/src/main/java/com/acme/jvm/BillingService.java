package com.acme.jvm;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

@Service
public class BillingService {

    private final RabbitTemplate rabbit;

    public BillingService(RabbitTemplate rabbit) {
        this.rabbit = rabbit;
    }

    void charge(String orderId) {
        rabbit.convertAndSend("billing.exchange", "invoices.issued", orderId);
    }
}
