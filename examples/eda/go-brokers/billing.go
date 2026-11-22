package main

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type BillingService struct {
	ch *amqp.Channel
}

func (b *BillingService) Charge(ctx context.Context, orderID string) error {
	return b.ch.PublishWithContext(ctx, "billing.exchange", "invoices.issued", false, false, amqp.Publishing{Body: []byte(orderID)})
}
