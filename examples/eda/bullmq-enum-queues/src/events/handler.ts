import { EventPattern } from '@nestjs/microservices';
import { JobQueue } from './enums';

export class EmailEventHandler {
  @EventPattern(JobQueue.EMAIL)
  onEmail(): void {}
}
