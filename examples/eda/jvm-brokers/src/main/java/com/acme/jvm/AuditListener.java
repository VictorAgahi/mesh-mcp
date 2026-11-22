package com.acme.jvm;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class AuditListener {

    private static final String AUDIT_QUEUE = "audit.events";

    @RabbitListener(queues = AUDIT_QUEUE) // consumes audit.events
    public void onAudit(String event) {
        System.err.println("audit " + event);
    }
}
