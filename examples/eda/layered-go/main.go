package main

import (
	"context"
	"os"
)

func main() {
	brokers := []string{"localhost:9092"}

	var pub Publisher = NewKafkaPublisher(brokers[0])
	if os.Getenv("AUDIT") == "1" {
		pub = NewAuditPublisher(brokers[0])
	}

	orders := NewOrderConsumer(brokers, NewOrderService(NewDefaultPricer(), pub))
	billing := NewBillingConsumer(brokers, NewBillingService(NewInvoiceWriter(brokers[0])))

	ctx := context.Background()
	go func() { _ = billing.Run(ctx) }()
	_ = orders.Run(ctx)
}
