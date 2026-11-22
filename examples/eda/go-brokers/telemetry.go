package main

import "github.com/nats-io/nats.go"

type Telemetry struct {
	nc *nats.Conn
}

func NewTelemetry(url string) (*Telemetry, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	return &Telemetry{nc: nc}, nil
}

func (t *Telemetry) Sample(cpu []byte) error {
	return t.nc.Publish("telemetry.cpu", cpu)
}

func (t *Telemetry) Listen() error {
	_, err := t.nc.Subscribe("devices.>", func(m *nats.Msg) {})
	return err
}

func (t *Telemetry) Export(data []byte) error {
	js, err := t.nc.JetStream()
	if err != nil {
		return err
	}
	_, err = js.Publish("telemetry.export", data)
	return err
}
