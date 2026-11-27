package com.acme.pay;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTemplate<String, VoidEvent> voidKafka;
    private final RabbitTemplate rabbit;

    public PaymentPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                            KafkaTemplate<String, VoidEvent> voidKafka, RabbitTemplate rabbit) {
        this.kafkaTemplate = kafkaTemplate;
        this.voidKafka = voidKafka;
        this.rabbit = rabbit;
    }

    void captured(String orderId, long amount) {
        kafkaTemplate.send("payments.captured", new PaymentEvent(orderId, amount));
    }

    // The payload is a method result: its type is the template's value type, VoidEvent.
    void voided(String orderId) {
        voidKafka.send("payments.voided", build(orderId));
    }

    private VoidEvent build(String orderId) {
        return new VoidEvent();
    }

    void auditKafka(AuditKafka a) {
        kafkaTemplate.send("audit", a);
    }

    void auditRabbit(AuditRabbit a) {
        rabbit.convertAndSend("audit", a);
    }
}
