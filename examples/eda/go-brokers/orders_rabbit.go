package main

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// OrderListener drains the `orders` queue and hands each delivery to billing.
type OrderListener struct {
	ch      *amqp.Channel
	billing *BillingService
}

func (l *OrderListener) Run(ctx context.Context) error {
	msgs, err := l.ch.Consume("orders", "billing", true, false, false, false, nil)
	if err != nil {
		return err
	}
	for m := range msgs {
		if err := l.billing.Charge(ctx, string(m.Body)); err != nil {
			return err
		}
	}
	return nil
}
