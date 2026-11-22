package com.acme.jvm;

import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import org.springframework.stereotype.Service;

@Service
public class NotificationService {

    private final SnsClient sns;

    public NotificationService(SnsClient sns) {
        this.sns = sns;
    }

    void announce(String orderId) {
        sns.publish(PublishRequest.builder().topicArn("arn:aws:sns:eu-west-1:123456789012:order-events").message(orderId).build());
    }
}
