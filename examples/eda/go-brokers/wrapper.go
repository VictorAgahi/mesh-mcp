package main

import (
	"github.com/nats-io/nats.go"
	"github.com/segmentio/kafka-go"
)

// Feed wraps a kafka-go reader behind its own Subscribe: the call below is a Kafka consumer,
// not a call of the NATS connection next to it.
type Feed struct {
	r *kafka.Reader
}

func (f *Feed) Subscribe(topic string) error { return nil }

func Follow(nc *nats.Conn, f *Feed) error {
	return f.Subscribe("feed.updates")
}
