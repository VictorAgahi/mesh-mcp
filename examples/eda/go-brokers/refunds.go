package main

import (
	"os"

	"github.com/nats-io/nats.go"
)

// The subject is `REFUNDS_SUBJECT` of the configuration (.env.example), not a literal.
func RequestRefund(nc *nats.Conn, body []byte) error {
	return nc.Publish(os.Getenv("REFUNDS_SUBJECT"), body)
}

func HandleRefunds(nc *nats.Conn) error {
	subject := os.Getenv("REFUNDS_SUBJECT")
	_, err := nc.Subscribe(subject, func(m *nats.Msg) {})
	return err
}
