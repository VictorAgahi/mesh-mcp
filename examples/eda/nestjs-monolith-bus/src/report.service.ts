import { Injectable } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';

@Injectable()
export class ReportService {
  constructor(private readonly emitter: EventEmitter2) {}

  generate() {
    this.emitter.emit('report.generated', { at: new Date().toISOString() });
  }

  purge() {
    this.emitter.emit('report.purged', { before: 30 });
  }
}
