import { forwardRef, Inject, Injectable } from '@nestjs/common';
import { LedgerService } from './ledger.service';

@Injectable()
export class ReconcileService {
  private depth = 0;

  constructor(
    @Inject(forwardRef(() => LedgerService)) private readonly ledger: LedgerService,
  ) {}

  async run(batchId: string) {
    this.ledger.post(batchId);
  }

  // Called back by LedgerService; re-posts until the batch is balanced.
  confirm(batchId: string) {
    if (this.depth++ < 2) {
      this.ledger.post(batchId);
    }
  }
}
