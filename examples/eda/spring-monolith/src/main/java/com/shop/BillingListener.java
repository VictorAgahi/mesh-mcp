package com.shop;

import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class BillingListener {

    private final BillingService billing;

    public BillingListener(BillingService billing) {
        this.billing = billing;
    }

    @EventListener
    public void onOrderPlaced(OrderPlacedEvent event) {
        billing.charge(event.orderId(), event.total());
    }
}
