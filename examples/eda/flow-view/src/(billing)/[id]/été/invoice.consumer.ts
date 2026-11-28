import { Controller } from '@nestjs/common';
import { EventPattern } from '@nestjs/microservices';
import { InvoiceService } from './invoice.service';

@Controller()
export class InvoiceConsumer {
  constructor(private readonly invoices: InvoiceService) {}

  @EventPattern('invoice.requested')
  async handle(dto: { invoiceId: string }) {
    await this.invoices.issue(dto);
  }
}
