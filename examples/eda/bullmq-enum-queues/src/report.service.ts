import { Injectable } from '@nestjs/common';
import { Queue } from 'bullmq';
import { JobQueue } from './queues';

@Injectable()
export class ReportService {
  private readonly queue = new Queue(JobQueue.REPORTS, { connection: { host: 'redis' } });

  async request(kind: string): Promise<void> {
    await this.queue.add('build', { kind });
  }
}
