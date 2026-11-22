package com.acme.jvm;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class OrderRabbitListener {

    private final BillingService billing;

    public OrderRabbitListener(BillingService billing) {
        this.billing = billing;
    }

    @RabbitListener(queues = "orders")
    public void onOrder(String orderId) {
        billing.charge(orderId);
    }
}
