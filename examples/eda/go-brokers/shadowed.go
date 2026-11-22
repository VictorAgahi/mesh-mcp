package main

import (
	"github.com/nats-io/nats.go"
)

// Bus has the method names of a NATS connection and nothing else.
type Bus struct{}

func (Bus) Publish(subject string, data []byte) error { return nil }
func (Bus) Subscribe(subject string, h nats.MsgHandler) {}

// Conn is a type of this file: it hides nothing of the library, and is not it.
type Conn struct{}

func (Conn) Publish(subject string, data []byte) error { return nil }

// Every call below is on a Bus or a Conn of this file, never on the nats.Conn the import brings.
func Shadowed(nc *nats.Conn, buses []Bus, local Conn) {
	for _, nc := range buses {
		nc.Publish("shadow.range", nil)
	}
	each := func(nc Bus) {
		nc.Publish("shadow.closure", nil)
	}
	each(Bus{})
	local.Publish("shadow.conn", nil)
	{
		nc := Bus{}
		nc.Subscribe("shadow.block", nil)
	}
}
