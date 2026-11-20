import { forwardRef, Inject, Injectable } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { ReconcileService } from './reconcile.service';

@Injectable()
export class LedgerService {
  constructor(
    @Inject('KAFKA_CLIENT') private readonly client: ClientKafka,
    @Inject(forwardRef(() => ReconcileService)) private readonly reconcile: ReconcileService,
  ) {}

  post(batchId: string) {
    this.client.emit('ledger.posted', { batchId });
    this.reconcile.confirm(batchId);
  }
}
