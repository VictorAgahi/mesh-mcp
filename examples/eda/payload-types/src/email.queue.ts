import { Job, Queue, Worker } from 'bullmq';

interface EmailJob {
  to: string;
  subject: string;
}

interface EmailJobV2 {
  to: string;
  subject: string;
  template: string;
}

export const emails = new Queue<EmailJob>('email');

export const emailWorker = new Worker('email', async (job: Job<EmailJobV2>) => {
  return job.data.template;
});
