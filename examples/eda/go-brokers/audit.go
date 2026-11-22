package main

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/nats-io/nats.go"
)

// `audit.events` is a RabbitMQ queue and a NATS subject: two channels.
func ConsumeAuditQueue(ctx context.Context, ch *amqp.Channel) error {
	_, err := ch.Consume("audit.events", "audit", true, false, false, false, nil)
	return err
}

func ConsumeAuditSubject(nc *nats.Conn) error {
	_, err := nc.QueueSubscribe("audit.events", "auditors", func(m *nats.Msg) {})
	return err
}
