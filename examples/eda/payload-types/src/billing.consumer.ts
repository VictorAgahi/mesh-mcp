import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { ShipmentEvent } from './order.events';

// Billing's own view of order.created: it expects customerId (never sent), reads total as a
// string (sent as a number) and ignores currency (sent, never read).
interface BillingOrder {
  orderId: string;
  total: string;
  customerId: string;
  note?: string;
}

@Controller()
export class BillingConsumer {
  @EventPattern('order.created')
  onCreated(@Payload() order: BillingOrder, @Ctx() ctx: KafkaContext) {
    return order.orderId;
  }

  @EventPattern('order.shipped')
  onShipped(@Payload() e: ShipmentEvent) {
    return e.carrier;
  }

  @EventPattern('order.cancelled')
  onCancelled(@Payload() e: { orderId: string }) {
    return e.orderId;
  }

  // `any`: not statically typed, counted, never a mismatch.
  @EventPattern('order.refunded')
  onRefunded(@Payload() e: any) {
    return e;
  }

  // Only an optional field the producer does not send: no finding.
  @EventPattern('order.noted')
  onNoted(@Payload() e: { orderId: string; note?: string }) {
    return e.orderId;
  }
}
