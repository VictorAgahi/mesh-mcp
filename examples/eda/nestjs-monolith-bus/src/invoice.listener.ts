import { Injectable } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';
import { InvoiceService } from './invoice.service';

@Injectable()
export class InvoiceListener {
  constructor(private readonly invoices: InvoiceService) {}

  @OnEvent('order.created')
  onOrderCreated(order: { orderId: string; total: number }) {
    this.invoices.issue(order);
  }
}
