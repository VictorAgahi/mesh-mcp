package main

import (
	"log"

	"github.com/nats-io/nats.go"
)

func WatchOutbox(nc *nats.Conn) error {
	_, err := nc.Subscribe("mail.outbox", func(m *nats.Msg) {
		log.Printf("mail queued: %s", m.Data)
	})
	return err
}
