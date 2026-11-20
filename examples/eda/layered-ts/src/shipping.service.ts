import { Injectable } from '@nestjs/common';
import { ShipmentPublisher } from './shipment.publisher';

@Injectable()
export class ShippingService {
  constructor(private readonly publisher: ShipmentPublisher) {}

  async schedule(orderId: string) {
    const slot = new Date(Date.now() + 86_400_000).toISOString();
    this.publisher.publishScheduled(orderId, slot);
  }
}
