import { Controller } from '@nestjs/common';
import { ClientKafka, EventPattern } from '@nestjs/microservices';

@Controller()
export class LedgerConsumer {
  constructor(private readonly client: ClientKafka) {}

  @EventPattern('invoice.issued')
  async post(dto: { invoiceId: string }) {
    this.client.emit('invoice.requested', dto);
  }
}
