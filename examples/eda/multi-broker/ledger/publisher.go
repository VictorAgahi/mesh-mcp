package main

import "github.com/segmentio/kafka-go"

func NewOrderWriter(addr string) *kafka.Writer {
	return &kafka.Writer{Addr: kafka.TCP(addr), Topic: "orders"}
}
