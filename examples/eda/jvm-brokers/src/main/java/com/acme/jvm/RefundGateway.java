package com.acme.jvm;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class RefundGateway {

    @Value("${app.queues.refunds}")
    private String refundsQueue;

    private final RabbitTemplate rabbit;

    public RefundGateway(RabbitTemplate rabbit) {
        this.rabbit = rabbit;
    }

    public void request(String orderId) {
        rabbit.convertAndSend(refundsQueue, orderId);
    }

    @RabbitListener(queues = "${app.queues.refunds}")
    public void onRefund(String orderId) {
        System.err.println("refund " + orderId);
    }
}
