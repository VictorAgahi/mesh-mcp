// The payload the order service publishes on order.created.
export interface OrderCreated {
  orderId: string;
  total: number;
  currency: string;
  note?: string;
}

// Shared by both ends of order.shipped: no finding.
export interface ShipmentEvent {
  orderId: string;
  carrier: string;
}
