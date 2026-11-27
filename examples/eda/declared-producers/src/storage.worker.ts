import { Processor } from '@nestjs/bullmq';
import { Queue } from './enums';

@Processor(Queue.STORAGE)
export class StorageWorker {
  async process() {}
}
