package main

// Pricer prices an order line.
type Pricer interface {
	Price(sku string, qty int) float64
}

type DefaultPricer struct{}

func NewDefaultPricer() *DefaultPricer { return &DefaultPricer{} }

func (p *DefaultPricer) Price(sku string, qty int) float64 {
	return float64(qty) * 9.99
}
