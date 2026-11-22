import { Processor, WorkerHost } from '@nestjs/bullmq';
import { LegacyQueue } from './queues';

// Consumes the legacy queue. Nothing produces it: JobQueue.EMAIL is another queue.
@Processor(LegacyQueue.EMAIL)
export class LegacyEmailProcessor extends WorkerHost {
  async process(): Promise<void> {}
}
