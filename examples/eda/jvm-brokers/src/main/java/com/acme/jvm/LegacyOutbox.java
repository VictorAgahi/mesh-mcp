package com.acme.jvm;

import javax.jms.Queue;
import org.apache.activemq.command.ActiveMQQueue;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

@Component
public class LegacyOutbox {

    private final JmsTemplate jms;

    public LegacyOutbox(JmsTemplate jms) {
        this.jms = jms;
    }

    void push(String body) {
        jms.send(new ActiveMQQueue("legacy.queue"), session -> session.createTextMessage(body));
    }

    Object pull() {
        return jms.receiveAndConvert("legacy.queue");
    }
}
