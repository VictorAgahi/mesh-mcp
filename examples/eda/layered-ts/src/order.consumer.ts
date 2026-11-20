import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { OrderService } from './order.service';

@Controller()
export class OrderConsumer {
  constructor(private readonly orders: OrderService) {}

  @EventPattern('order.requested')
  async handle(@Payload() dto: { orderId: string; sku: string }) {
    await this.orders.place(dto);
  }
}
