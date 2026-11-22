package main

import (
	"context"

	"github.com/nats-io/nats.go"
)

type MailService struct {
	nc *nats.Conn
}

func (s *MailService) Send(ctx context.Context, invoice string) error {
	return s.nc.Publish("mail.outbox", []byte(invoice))
}
