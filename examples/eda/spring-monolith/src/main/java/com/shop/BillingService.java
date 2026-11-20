package com.shop;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BillingService {

    @Autowired
    private PaymentPublisher payments;

    public void charge(String orderId, double total) {
        payments.publishCharged(orderId, total);
    }

    public void reconcile() {
        payments.publishReconciled();
    }
}
