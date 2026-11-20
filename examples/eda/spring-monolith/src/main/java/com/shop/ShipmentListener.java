package com.shop;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
public class ShipmentListener {

    private final ShipmentService shipments;

    public ShipmentListener(ShipmentService shipments) {
        this.shipments = shipments;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onFulfilmentStarted(FulfilmentStartedEvent event) {
        shipments.dispatch(event.orderId());
    }

    // The topic name lives in application.yml (app.topics.shipments).
    @KafkaListener(topics = "${app.topics.shipments}")
    public void onDispatched(String message) {
        System.err.println("shipment dispatched: " + message);
    }
}
