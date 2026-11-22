package main

import (
	"context"

	"cloud.google.com/go/pubsub"
)

// Archive publishes each invoice to a Pub/Sub topic and drains it through a subscription the
// file creates on that very topic.
func Archive(ctx context.Context, client *pubsub.Client, invoice []byte) error {
	_, err := client.Topic("invoices.archive").Publish(ctx, &pubsub.Message{Data: invoice}).Get(ctx)
	return err
}

func Drain(ctx context.Context, client *pubsub.Client) error {
	topic := client.Topic("invoices.archive")
	sub, err := client.CreateSubscription(ctx, "archive-sub", pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		return err
	}
	return sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) { m.Ack() }) // archive-sub drains invoices.archive
}
