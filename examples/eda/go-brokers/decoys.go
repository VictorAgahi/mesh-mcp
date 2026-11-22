package main

import (
	"context"
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

// Bridge is not a broker client: its Publish is a method of an unrelated interface.
type Bridge interface {
	Publish(channel string, payload []byte)
}

// Only the last call of Relay is a site. Every other call is on an unrelated type, a string in a
// log line, a pattern, or a name the source does not show.
func Relay(ctx context.Context, bridge Bridge, nc *nats.Conn, ch *amqp.Channel, rdb *redis.Client, subject string, body []byte) {
	bridge.Publish("orders.legacy", body)
	log.Println("nc.Publish(\"orders.logged\", body) failed")
	nc.Publish(subject, body)
	nc.Publish(fmt.Sprintf("orders.%s", subject), body)
	rdb.Publish(ctx, "alerts.*", body)
	ch.Publish("ex.wild", "audit.#", false, false, amqp.Publishing{Body: body})
	fallback := "orders.default"
	if v := os.Getenv("RELAY_SUBJECT"); v != "" {
		fallback = v
	}
	nc.Publish(fallback, body)
	ch.Publish("", "legacy.relay", false, false, amqp.Publishing{Body: body})
}
