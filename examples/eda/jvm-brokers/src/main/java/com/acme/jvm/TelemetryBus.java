package com.acme.jvm;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import org.springframework.stereotype.Component;

@Component
public class TelemetryBus {

    private final Connection nats;

    public TelemetryBus(Connection nats) {
        this.nats = nats;
    }

    void emit(byte[] sample) {
        nats.publish("telemetry.cpu", sample);
    }

    void listen() {
        Dispatcher dispatcher = nats.createDispatcher(msg -> {});
        dispatcher.subscribe("telemetry.cpu");
        nats.createDispatcher(msg -> {}).subscribe("audit.events");
    }
}
