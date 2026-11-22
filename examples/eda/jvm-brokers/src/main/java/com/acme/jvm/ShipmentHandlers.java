package com.acme.jvm;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
@RabbitListener(queues = "shipments")
public class ShipmentHandlers {

    private final AmqpTemplate amqp;

    public ShipmentHandlers(AmqpTemplate amqp) {
        this.amqp = amqp;
    }

    @RabbitHandler
    public void onCreated(ShipmentCreated created) {
        amqp.convertAndSend("shipments.ack", created.id());
    }

    @RabbitHandler
    public void onCancelled(ShipmentCancelled cancelled) {
        System.err.println("cancelled " + cancelled.id());
    }

    record ShipmentCreated(String id) {}

    record ShipmentCancelled(String id) {}
}
