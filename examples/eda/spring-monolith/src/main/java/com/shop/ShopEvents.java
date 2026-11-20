package com.shop;

// In-process application events (Spring ApplicationEventPublisher payloads).
record OrderPlacedEvent(String orderId, double total) {}

record FulfilmentStartedEvent(String orderId) {}
