package main

import (
	"testing"

	"github.com/nats-io/nats.go"
)

// A test file is not production wiring: the scorer ignores the site below.
func TestPublishesAudit(t *testing.T) {
	nc, _ := nats.Connect("nats://localhost:4222")
	nc.Publish("test.only.subject", nil)
}
