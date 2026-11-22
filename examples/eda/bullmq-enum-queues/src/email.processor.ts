import { Processor, WorkerHost } from '@nestjs/bullmq';
import { JobQueue } from './queues';
import { ReportService } from './report.service';

@Processor(JobQueue.EMAIL)
export class EmailProcessor extends WorkerHost {
  constructor(private readonly reports: ReportService) {
    super();
  }

  async process(): Promise<void> {
    await this.reports.request('daily');
  }
}
