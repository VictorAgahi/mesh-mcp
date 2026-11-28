import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { OrderCreated, ShipmentEvent } from './order.events';

@Controller()
export class BillingConsumer {
  @EventPattern('order.created')
  onCreated(@Payload() order: OrderCreated) {
    return order.orderId;
  }

  @EventPattern('order.shipped')
  onShipped(@Payload() e: ShipmentEvent) {
    return e.carrier;
  }
}
