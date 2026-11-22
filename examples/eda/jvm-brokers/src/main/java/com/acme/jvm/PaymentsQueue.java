package com.acme.jvm;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import org.springframework.stereotype.Component;

@Component
public class PaymentsQueue {

    private final SqsClient sqs;

    public PaymentsQueue(SqsClient sqs) {
        this.sqs = sqs;
    }

    void push(String body) {
        sqs.sendMessage(SendMessageRequest.builder().queueUrl("https://sqs.eu-west-1.amazonaws.com/123456789012/payments").messageBody(body).build());
    }

    void poll() {
        sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl("https://sqs.eu-west-1.amazonaws.com/123456789012/payments").build());
    }
}
