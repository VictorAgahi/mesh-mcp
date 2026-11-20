import { Injectable } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';

@Injectable()
export class LedgerListener {
  @OnEvent('invoice.issued')
  onInvoiceIssued(invoice: { invoiceId: string; total: number }) {
    console.error(`ledger entry for ${invoice.invoiceId}`);
  }
}
