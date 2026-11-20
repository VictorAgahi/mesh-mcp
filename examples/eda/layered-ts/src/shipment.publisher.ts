import { Inject, Injectable } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';

@Injectable()
export class ShipmentPublisher {
  constructor(@Inject('KAFKA_CLIENT') private readonly client: ClientKafka) {}

  publishScheduled(orderId: string, slot: string) {
    this.client.emit('shipment.scheduled', { orderId, slot });
  }
}
