import { Injectable } from '@nestjs/common';
import { ShippingRepo } from './shipping.repo';

@Injectable()
export class ShipmentService {
  constructor(private readonly repo: ShippingRepo) {}

  async ship(orderId: string): Promise<void> {
    await this.repo.markShipped(orderId);
  }
}
