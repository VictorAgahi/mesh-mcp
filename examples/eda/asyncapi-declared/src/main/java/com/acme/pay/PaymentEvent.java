package com.acme.pay;

// Published and consumed on payments.captured; the declared schema says the same fields.
public record PaymentEvent(String orderId, long amount) {}
