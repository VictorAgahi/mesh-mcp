import { Body, Controller, Post } from '@nestjs/common';
import { OrderService } from './order.service';

@Controller('orders')
export class OrderController {
  constructor(private readonly orders: OrderService) {}

  @Post()
  async create(@Body() dto: { orderId: string; sku: string }) {
    await this.orders.place(dto);
    return { accepted: true };
  }
}
