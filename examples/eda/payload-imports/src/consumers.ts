import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { Refund as Order } from './refund';
import { VoidedView } from './views';
import { Order2 } from '@acme/contracts';
import Parcel from './shipment';

@Controller()
export class Consumers {
  // `Order` is the imported Refund, never the Order class of src/order.ts: no mismatch.
  @EventPattern('refund.issued')
  onIssued(@Payload() e: Order) {
    return e.amount;
  }

  @EventPattern('refund.voided')
  onVoided(@Payload() e: VoidedView) {
    return e.reason;
  }

  // Imported from a package that is not indexed: not compared, never src/order.ts's Order2.
  @EventPattern('order2.placed')
  onPlaced(@Payload() e: Order2) {
    return e.id;
  }

  @EventPattern('shipment.sent')
  onShipped(@Payload() e: Parcel) {
    return e.carrier;
  }

  @EventPattern('quoted.keys')
  onQuoted(@Payload() e: { orderId: string; total: number }) {
    return e.total;
  }
}
