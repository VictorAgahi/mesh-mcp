import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { JobQueue } from './queues';

// Registering a queue makes it injectable; it does not publish to it.
@Module({
  imports: [BullModule.registerQueue({ name: JobQueue.EMAIL }, { name: JobQueue.AUDIT })],
})
export class AppModule {}
