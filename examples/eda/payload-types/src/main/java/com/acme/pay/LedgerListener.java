package com.acme.pay;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class LedgerListener {

    @KafkaListener(topics = "payments.captured", groupId = "ledger")
    public void onCaptured(@Payload PaymentCaptured evt) {
    }

    @KafkaListener(topics = "payments.voided", groupId = "ledger")
    public void onVoided(ConsumerRecord<String, VoidEvent> rec) {
    }

    @KafkaListener(topics = "audit", groupId = "ledger")
    public void onAuditKafka(AuditKafka a) {
    }

    @RabbitListener(queues = "audit")
    public void onAuditRabbit(AuditRabbit a) {
    }
}
