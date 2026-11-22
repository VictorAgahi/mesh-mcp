import { Injectable } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { JobQueue } from './queues';

// Nothing in the fixture consumes the audit queue.
@Injectable()
export class AuditService {
  constructor(@InjectQueue(JobQueue.AUDIT) private readonly auditQueue: Queue) {}

  async record(event: string): Promise<void> {
    await this.auditQueue.add('record', { event });
  }
}
