import { Injectable } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { ReportService } from './report.service';

@Injectable()
export class ReportScheduler {
  constructor(private readonly reports: ReportService) {}

  @Cron('0 * * * *')
  hourly() {
    this.reports.generate();
  }

  @Cron('0 3 * * *')
  nightlyPurge() {
    this.reports.purge();
  }
}
