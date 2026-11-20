import { EventEmitter } from 'events';
import { Injectable, Logger } from '@nestjs/common';

// In-process metrics counters.
@Injectable()
export class MetricsService {
  private readonly logger = new Logger(MetricsService.name);
  private readonly local = new EventEmitter();

  record(name: string) {
    this.logger.log(`order.requested seen: ${name}`);
    this.local.emit('metrics.tick', name);
  }
}
