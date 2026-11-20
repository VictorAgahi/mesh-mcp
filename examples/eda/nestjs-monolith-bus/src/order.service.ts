import { Injectable } from '@nestjs/common';
import { OrderEvents } from './order.events';

@Injectable()
export class OrderService {
  constructor(private readonly events: OrderEvents) {}

  create(dto: { orderId: string; total: number }) {
    const order = { ...dto, status: 'created' };
    this.events.announce(order);
    return order;
  }
}
