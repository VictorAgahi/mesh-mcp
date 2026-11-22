package com.acme.jvm;

import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
public class PriceFeed {

    private final StringRedisTemplate redis;

    public PriceFeed(StringRedisTemplate redis) {
        this.redis = redis;
    }

    void publishPrice(String payload) {
        redis.convertAndSend("price.updates", payload);
    }

    void publishAlert(String payload) {
        redis.convertAndSend("alerts.fired", payload);
    }

    void listen(RedisMessageListenerContainer container, MessageListener adapter) {
        container.addMessageListener(adapter, new ChannelTopic("price.updates"));
        container.addMessageListener(adapter, new PatternTopic("metrics.*"));
    }
}
