package com.shop;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class OrderEventPublisher {

    private final ApplicationEventPublisher events;

    public OrderEventPublisher(ApplicationEventPublisher events) {
        this.events = events;
    }

    void announce(String orderId, double total) {
        events.publishEvent(new OrderPlacedEvent(orderId, total));
    }
}
