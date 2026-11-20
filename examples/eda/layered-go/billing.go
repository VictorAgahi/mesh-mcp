package main

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type InvoiceWriter struct {
	writer *kafka.Writer
}

func NewInvoiceWriter(addr string) *InvoiceWriter {
	return &InvoiceWriter{writer: &kafka.Writer{Addr: kafka.TCP(addr), Topic: "invoices.issued"}}
}

func (w *InvoiceWriter) Write(ctx context.Context, orderID string) error {
	return w.writer.WriteMessages(ctx, kafka.Message{Key: []byte(orderID)})
}

type BillingService struct {
	invoices *InvoiceWriter
}

func NewBillingService(invoices *InvoiceWriter) *BillingService {
	return &BillingService{invoices: invoices}
}

func (s *BillingService) Invoice(ctx context.Context, orderID string) error {
	return s.invoices.Write(ctx, orderID)
}

type BillingConsumer struct {
	reader  *kafka.Reader
	billing *BillingService
}

func NewBillingConsumer(brokers []string, billing *BillingService) *BillingConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: "orders.priced", GroupID: "billing"})
	return &BillingConsumer{reader: r, billing: billing}
}

func (c *BillingConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			return err
		}
		if err := c.billing.Invoice(ctx, string(msg.Key)); err != nil {
			return err
		}
	}
}
