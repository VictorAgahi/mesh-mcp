import { Injectable } from '@nestjs/common';
import { InjectQueue, Processor, WorkerHost } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
// An enum of a package that is not part of this workspace: its values are unknown here.
import { ExternalQueue } from '@acme/queues';

@Injectable()
export class BillingService {
  constructor(@InjectQueue(ExternalQueue.BILLING) private readonly billing: Queue) {}
}

@Processor(ExternalQueue.BILLING)
export class BillingProcessor extends WorkerHost {
  async process(): Promise<void> {}
}
