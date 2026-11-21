import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { ShipmentService } from './shipment.service';

@Controller()
export class ShipmentConsumer {
  constructor(private readonly shipments: ShipmentService) {}

  @EventPattern('shipment.requested')
  async handle(@Payload() dto: { orderId: string }) {
    await this.shipments.ship(dto.orderId);
  }
}
