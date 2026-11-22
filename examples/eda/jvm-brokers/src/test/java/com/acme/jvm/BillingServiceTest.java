package com.acme.jvm;

import org.springframework.amqp.rabbit.core.RabbitTemplate;

class BillingServiceTest {

    private RabbitTemplate rabbit;

    void sendsAnInvoice() {
        rabbit.convertAndSend("billing.exchange", "invoices.test", "order-1");
    }
}
