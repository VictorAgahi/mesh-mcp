package com.shop;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

@Service
public class FulfilmentService {

    private final ApplicationEventPublisher events;

    public FulfilmentService(ApplicationEventPublisher events) {
        this.events = events;
    }

    public void start(String orderId) {
        events.publishEvent(new FulfilmentStartedEvent(orderId));
    }
}
