package main

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type OrderConsumer struct {
	reader *kafka.Reader
	svc    *OrderService
}

func NewOrderConsumer(brokers []string, svc *OrderService) *OrderConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: "orders.placed", GroupID: "pricing"})
	return &OrderConsumer{reader: r, svc: svc}
}

func (c *OrderConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			return err
		}
		if err := c.svc.Handle(ctx, string(msg.Key), 1); err != nil {
			return err
		}
	}
}
