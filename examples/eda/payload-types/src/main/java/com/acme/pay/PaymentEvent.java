package com.acme.pay;

// What the payment service publishes on payments.captured.
public record PaymentEvent(String orderId, long amount) {}
