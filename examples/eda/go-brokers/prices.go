package main

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Alerts struct {
	rdb *redis.Client
}

func (a *Alerts) Raise(ctx context.Context, sku string) error {
	return a.rdb.Publish(ctx, "alerts.fired", sku).Err()
}

type PriceFeed struct {
	rdb    *redis.Client
	alerts *Alerts
}

func (f *PriceFeed) Set(ctx context.Context, sku string, price string) error {
	return f.rdb.Publish(ctx, "price.updates", sku+"="+price).Err()
}

func (f *PriceFeed) Watch(ctx context.Context) error {
	sub := f.rdb.Subscribe(ctx, "price.updates")
	for m := range sub.Channel() {
		if err := f.alerts.Raise(ctx, m.Payload); err != nil {
			return err
		}
	}
	return nil
}

func (f *PriceFeed) Metrics(ctx context.Context) {
	f.rdb.PSubscribe(ctx, "metrics.*")
}
