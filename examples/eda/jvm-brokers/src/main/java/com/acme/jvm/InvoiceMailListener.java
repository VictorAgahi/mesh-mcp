package com.acme.jvm;

import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class InvoiceMailListener {

    private final MailService mail;

    public InvoiceMailListener(MailService mail) {
        this.mail = mail;
    }

    @RabbitListener(bindings = @QueueBinding(value = @Queue(name = "invoice-mail"), exchange = @Exchange("billing.exchange"), key = "invoices.issued"))
    public void onInvoice(String invoiceId) {
        mail.send(invoiceId);
    }
}
