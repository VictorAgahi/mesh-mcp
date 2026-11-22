package com.acme.jvm;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

// No site here but the last: every other call is on an unrelated class, a string in a log line,
// a pattern or a name the source does not show. The last one sends `legacy.pp`, whose third
// argument is a post-processor, so `body` is the payload, not a channel.
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

    void run(String queueName, PublishRequest prepared, Object body,
             org.springframework.amqp.core.MessagePostProcessor pp) {
        bridge.convertAndSend("orders.legacy", body);
        System.err.println("rabbit.convertAndSend(\"orders.logged\", body) failed");
        rabbit.convertAndSend(queueName, body);
        jms.convertAndSend(names.get(), body);
        sns.publish(prepared);
        rabbit.convertAndSend("ex.wild", "audit.#", body);
        rabbit.convertAndSend("legacy.pp", "body", pp);
    }
}
