import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { ReconcileService } from './reconcile.service';

@Controller()
export class ReconcileConsumer {
  constructor(private readonly reconcile: ReconcileService) {}

  @EventPattern('ledger.reconcile')
  async onReconcile(@Payload() batch: { batchId: string }) {
    await this.reconcile.run(batch.batchId);
  }
}
