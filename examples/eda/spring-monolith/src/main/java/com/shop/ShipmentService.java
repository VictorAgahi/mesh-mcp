package com.shop;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class ShipmentService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public ShipmentService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void dispatch(String orderId) {
        kafkaTemplate.send("shipments.dispatched", orderId);
    }
}
