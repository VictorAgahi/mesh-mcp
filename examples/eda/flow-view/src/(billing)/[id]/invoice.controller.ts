import { Controller, Post } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';

@Controller('invoices')
export class InvoiceController {
  constructor(private readonly client: ClientKafka) {}

  @Post('{id}/retry')
  retry(dto: { invoiceId: string }) {
    this.client.emit('invoice.requested', dto);
  }
}
