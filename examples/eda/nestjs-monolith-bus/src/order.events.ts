import { Injectable } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';

@Injectable()
export class OrderEvents {
  constructor(private readonly emitter: EventEmitter2) {}

  announce(order: { orderId: string; total: number }) {
    this.emitter.emit('order.created', order);
  }
}
