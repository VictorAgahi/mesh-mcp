import { Injectable } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { JobQueue } from './queues';

@Injectable()
export class EmailService {
  constructor(@InjectQueue(JobQueue.EMAIL) private readonly emailQueue: Queue) {}

  async send(to: string): Promise<void> {
    await this.emailQueue.add('send', { to });
  }
}
