package com.acme.pay;

// Same channel name as AuditRabbit, on Kafka: the two brokers are never joined.
public class AuditKafka {
    private String actor;
    private String action;
}
