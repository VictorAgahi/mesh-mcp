import { Job, Queue, Worker } from 'bullmq';

// `priority` is not in the declared EmailJob schema of legacy/asyncapi.v2.yaml.
interface EmailJob {
  to: string;
  subject: string;
  priority: number;
}

export const emails = new Queue<EmailJob>('email');

export const emailWorker = new Worker('email', async (job: Job<EmailJob>) => {
  return job.data.to;
});
