import { Controller } from '@nestjs/common';
import { EventPattern } from '@nestjs/microservices';

@Controller()
export class LegacyOrders {
  @EventPattern('orders')
  handle(): void {}
}
