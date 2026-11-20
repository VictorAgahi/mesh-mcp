package main

import "context"

type OrderService struct {
	pricer Pricer
	pub    Publisher
}

func NewOrderService(pricer Pricer, pub Publisher) *OrderService {
	return &OrderService{pricer: pricer, pub: pub}
}

func (s *OrderService) Handle(ctx context.Context, orderID string, qty int) error {
	total := s.pricer.Price(orderID, qty)
	return s.pub.Publish(ctx, orderID, total)
}
