import { Injectable } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { JobQueue as Q } from './queues';

// The enum is imported under another name: the import still binds it to src/queues.ts.
@Injectable()
export class AliasService {
  constructor(@InjectQueue(Q.REPORTS) private readonly reports: Queue) {}
}
