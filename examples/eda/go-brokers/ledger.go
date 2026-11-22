package main

import "github.com/segmentio/kafka-go"

// `orders` is a Kafka topic too: three channels now share the name.
func NewOrderWriter(addr string) *kafka.Writer {
	return &kafka.Writer{Addr: kafka.TCP(addr), Topic: "orders"}
}
