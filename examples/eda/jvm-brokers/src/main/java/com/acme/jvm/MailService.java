package com.acme.jvm;

import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;

@Service
public class MailService {

    private final JmsTemplate jms;

    public MailService(JmsTemplate jms) {
        this.jms = jms;
    }

    void send(String invoiceId) {
        jms.convertAndSend("mail.outbox", invoiceId);
    }
}
