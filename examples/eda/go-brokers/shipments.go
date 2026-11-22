package main

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The Kafka clients the Go extractor did not read before: sarama's consumer group and franz-go.
func ConsumeDispatched(ctx context.Context, group sarama.ConsumerGroup, h sarama.ConsumerGroupHandler) error {
	return group.Consume(ctx, []string{"shipments.dispatched"}, h)
}

func NewShipmentsClient() (*kgo.Client, error) {
	return kgo.NewClient(kgo.ConsumeTopics("shipments.returned"), kgo.DefaultProduceTopic("shipments.ack"))
}

func Ledger(ctx context.Context, cl *kgo.Client) {
	cl.Produce(ctx, &kgo.Record{Topic: "shipments.ledger"}, nil)
}
