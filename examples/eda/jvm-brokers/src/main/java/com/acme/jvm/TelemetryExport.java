package com.acme.jvm;

import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import org.springframework.stereotype.Component;

@Component
public class TelemetryExport {

    private final PubSubTemplate pubSub;

    public TelemetryExport(PubSubTemplate pubSub) {
        this.pubSub = pubSub;
    }

    void export(String payload) {
        pubSub.publish("telemetry-events", payload);
    }
}
