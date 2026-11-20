import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { ShippingService } from './shipping.service';

@Controller()
export class ShippingConsumer {
  constructor(private readonly shipping: ShippingService) {}

  @EventPattern('stock.reserved')
  async onStockReserved(@Payload() event: { orderId: string }) {
    await this.shipping.schedule(event.orderId);
  }
}
