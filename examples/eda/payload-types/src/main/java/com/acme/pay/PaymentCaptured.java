package com.acme.pay;

// The ledger's view of payments.captured: amount as text, and a method nobody sends.
public class PaymentCaptured {
    private String orderId;
    private String amount;
    private String method;
    private static final String KIND = "captured";
}
