package main

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// Publisher has two implementations; main.go picks one at startup.
type Publisher interface {
	Publish(ctx context.Context, orderID string, total float64) error
}

type KafkaPublisher struct {
	writer *kafka.Writer
}

func NewKafkaPublisher(addr string) *KafkaPublisher {
	return &KafkaPublisher{writer: &kafka.Writer{Addr: kafka.TCP(addr), Topic: "orders.priced"}}
}

func (p *KafkaPublisher) Publish(ctx context.Context, orderID string, total float64) error {
	return p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(orderID)})
}

type AuditPublisher struct {
	writer *kafka.Writer
}

func NewAuditPublisher(addr string) *AuditPublisher {
	return &AuditPublisher{writer: &kafka.Writer{Addr: kafka.TCP(addr), Topic: "orders.audit"}}
}

func (p *AuditPublisher) Publish(ctx context.Context, orderID string, total float64) error {
	return p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(orderID)})
}
