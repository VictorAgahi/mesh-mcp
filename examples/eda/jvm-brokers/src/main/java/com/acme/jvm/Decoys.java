package com.acme.jvm;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

// Nothing in this file is a broker site: every call is either on an unrelated class, a string
// in a log line, or a name the source does not show.
@Component
public class Decoys {

    interface LegacyBridge {
        void convertAndSend(String channel, Object payload);
    }

    private final LegacyBridge bridge;
    private final RabbitTemplate rabbit;
    private final JmsTemplate jms;
    private final SnsClient sns;
    private final java.util.function.Supplier<String> names;

    public Decoys(LegacyBridge bridge, RabbitTemplate rabbit, JmsTemplate jms, SnsClient sns,
                  java.util.function.Supplier<String> names) {
        this.bridge = bridge;
        this.rabbit = rabbit;
        this.jms = jms;
        this.sns = sns;
        this.names = names;
    }

    void run(String queueName, PublishRequest prepared, Object body) {
        bridge.convertAndSend("orders.legacy", body);
        System.err.println("rabbit.convertAndSend(\"orders.logged\", body) failed");
        rabbit.convertAndSend(queueName, body);
        jms.convertAndSend(names.get(), body);
        sns.publish(prepared);
    }
}
