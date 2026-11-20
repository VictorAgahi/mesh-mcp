package com.shop;

import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final OrderEventPublisher publisher;

    public OrderService(OrderEventPublisher publisher) {
        this.publisher = publisher;
    }

    public void place(String orderId, double total) {
        publisher.announce(orderId, total);
    }
}
