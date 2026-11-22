package main

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// InvoiceMailer binds its own queue to the `invoices.issued` routing key and mails each invoice.
type InvoiceMailer struct {
	ch   *amqp.Channel
	mail *MailService
}

func (m *InvoiceMailer) Run(ctx context.Context) error {
	q, err := m.ch.QueueDeclare("mail-invoices", true, false, false, false, nil)
	if err != nil {
		return err
	}
	if err := m.ch.QueueBind(q.Name, "invoices.issued", "billing.exchange", false, nil); err != nil {
		return err
	}
	msgs, err := m.ch.Consume(q.Name, "mailer", true, false, false, false, nil) // the queue is bound to invoices.issued above
	if err != nil {
		return err
	}
	for d := range msgs {
		if err := m.mail.Send(ctx, string(d.Body)); err != nil {
			return err
		}
	}
	return nil
}
