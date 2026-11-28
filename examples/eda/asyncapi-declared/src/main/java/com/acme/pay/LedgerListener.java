package com.acme.pay;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class LedgerListener {

    @KafkaListener(topics = "payments.captured", groupId = "ledger")
    public void onCaptured(@Payload PaymentEvent evt) {
    }

    // A placeholder no configuration file resolves: not a channel, counted as unresolved.
    @KafkaListener(topics = "${ledger.parking.topic}", groupId = "ledger")
    public void onParked(String raw) {
    }
}
