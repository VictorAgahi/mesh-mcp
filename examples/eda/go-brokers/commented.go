package main

import (
	"log"
	// "github.com/nats-io/nats.go"
)

// The import of the NATS client is commented out: nothing makes `nc` the library's connection,
// whatever its type is spelled like.
func Commented(nc *nats.Conn) {
	log.Println("publishing")
	nc.Publish("commented.out", nil)
}
