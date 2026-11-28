import { Injectable } from '@nestjs/common';
import { InvoicePublisher } from './invoice.publisher';

@Injectable()
export class InvoiceService {
  constructor(private readonly publisher: InvoicePublisher) {}

  async issue(dto: { invoiceId: string }) {
    this.publisher.publishIssued(dto);
    this.publisher.publishDead(dto);
  }
}
