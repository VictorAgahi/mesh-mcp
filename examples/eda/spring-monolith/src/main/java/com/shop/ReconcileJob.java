package com.shop;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ReconcileJob {

    private final BillingService billing;

    public ReconcileJob(BillingService billing) {
        this.billing = billing;
    }

    @Scheduled(cron = "0 0 * * * *")
    public void hourly() {
        billing.reconcile();
    }
}
