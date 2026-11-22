import { Worker } from 'bullmq';
import { JobQueue } from './queues';

export const reportWorker = new Worker(
  JobQueue.REPORTS,
  async () => {
    // build the report
  },
  { connection: { host: 'redis' } },
);
