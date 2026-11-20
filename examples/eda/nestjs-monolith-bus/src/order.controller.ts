import { Body, Controller, Post } from '@nestjs/common';
import { OrderService } from './order.service';

@Controller('orders')
export class OrderController {
  constructor(private readonly orders: OrderService) {}

  @Post()
  create(@Body() dto: { orderId: string; total: number }) {
    return this.orders.create(dto);
  }
}
