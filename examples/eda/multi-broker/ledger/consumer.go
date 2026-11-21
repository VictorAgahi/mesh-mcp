package main

import "github.com/segmentio/kafka-go"

func NewOrderReader(brokers []string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: "orders", GroupID: "ledger"})
}

func NewInvoiceReader(brokers []string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: "invoices", GroupID: "ledger"})
}
