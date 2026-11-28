// What the order service publishes on order.created, and what billing reads.
export interface OrderCreated {
  orderId: string;
  total: number;
  currency: string;
}

// Shared by both ends of order.shipped: declared the same way in asyncapi.yaml.
export interface ShipmentEvent {
  orderId: string;
  carrier: string;
}
