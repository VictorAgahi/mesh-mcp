package com.acme.jvm;

// import org.springframework.amqp.rabbit.core.RabbitTemplate;  (kept for the migration notes)
import java.util.List;
import org.springframework.stereotype.Component;

// Nothing in this file is a broker site: its RabbitTemplate is a nested class, and the loop
// and lambda variables hide it.
@Component
public class Shadowed {

    static class RabbitTemplate {
        void convertAndSend(String queue, Object message) {}
    }

    interface Sink {
        void convertAndSend(String queue, Object message);
    }

    private final RabbitTemplate rabbit = new RabbitTemplate();

    void nested(Object m) {
        rabbit.convertAndSend("shadow.nested", m);
    }

    void each(List<Sink> sinks, Object m) {
        for (Sink rabbit : sinks) { rabbit.convertAndSend("shadow.each", m); }
    }

    void lambda(List<Sink> sinks, Object m) {
        sinks.forEach(rabbit -> rabbit.convertAndSend("shadow.lambda", m));
    }
}
