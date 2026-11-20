import { Injectable } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';

@Injectable()
export class InvoiceService {
  constructor(private readonly emitter: EventEmitter2) {}

  issue(order: { orderId: string; total: number }) {
    const invoice = { invoiceId: `INV-${order.orderId}`, total: order.total };
    this.emitter.emit('invoice.issued', invoice);
  }
}
