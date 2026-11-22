package com.acme.jvm;

import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderQueues {

    private final SqsTemplate sqs;
    private final NotificationService notifications;

    public OrderQueues(SqsTemplate sqs, NotificationService notifications) {
        this.sqs = sqs;
        this.notifications = notifications;
    }

    public void enqueue(String orderId) {
        sqs.send("orders", orderId);
    }

    @SqsListener("orders")
    public void onQueued(String orderId) {
        notifications.announce(orderId);
    }
}
