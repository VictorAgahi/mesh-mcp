import { Inject, Injectable } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { OrderCreated, ShipmentEvent } from './order.events';

@Injectable()
export class OrderPublisher {
  constructor(@Inject('KAFKA') private readonly client: ClientKafka) {}

  created(orderId: string, total: number) {
    const evt: OrderCreated = { orderId, total, currency: 'EUR' };
    this.client.emit('order.created', evt);
  }

  shipped(e: ShipmentEvent) {
    this.client.emit('order.shipped', e);
  }

  // Serialised by hand: the payload type is not statically visible.
  cancelled(orderId: string) {
    this.client.emit('order.cancelled', JSON.stringify({ orderId }));
  }

  refunded(orderId: string) {
    this.client.emit('order.refunded', { orderId, amount: 10 });
  }

  noted(orderId: string) {
    this.client.emit('order.noted', { orderId });
  }
}
