import { Inject, Injectable } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';

@Injectable()
export class StockPublisher {
  constructor(@Inject('KAFKA_CLIENT') private readonly client: ClientKafka) {}

  publishReserved(payload: { orderId: string; sku: string; qty: number }) {
    this.client.emit('stock.reserved', payload);
  }
}
