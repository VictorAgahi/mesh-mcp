import { Injectable } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';

@Injectable()
export class ReportListener {
  @OnEvent('report.generated')
  onGenerated(payload: { at: string }) {
    console.error(`report generated at ${payload.at}`);
  }

  @OnEvent('report.purged')
  onPurged(payload: { before: number }) {
    console.error(`purged reports older than ${payload.before} days`);
  }
}
