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

  // Heard by the declared channel `order.audit.{region}`: a wildcard match, not an extra channel.
  auditedInEurope(orderId: string) {
    this.client.emit('order.audit.eu', { orderId });
  }

  // Published but declared nowhere.
  legacy(orderId: string) {
    this.client.emit('order.legacy', { orderId });
  }
}
