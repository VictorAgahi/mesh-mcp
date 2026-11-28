import { Inject, Injectable } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';

@Injectable()
export class InvoicePublisher {
  constructor(@Inject('KAFKA_CLIENT') private readonly client: ClientKafka) {}

  publishIssued(payload: { invoiceId: string }) {
    this.client.emit('invoice.issued', payload);
  }

  publishDead(payload: { invoiceId: string }) {
    this.client.emit('${INVOICE_DEAD_TOPIC}', payload);
  }
}
