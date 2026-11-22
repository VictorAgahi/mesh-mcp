package com.acme.jvm;

import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

@Component
public class MailOutboxListener {

    @JmsListener(destination = "mail.outbox")
    public void deliver(String body) {
        System.err.println("deliver " + body);
    }
}
