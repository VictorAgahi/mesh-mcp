import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { EmailNotifier, NOTIFIER, SmsNotifier } from './notifiers';
import { NotificationService } from './notification.service';

@Module({
  imports: [
    ClientsModule.register([
      { name: 'KAFKA_CLIENT', transport: Transport.KAFKA, options: { client: { brokers: ['localhost:9092'] } } },
    ]),
  ],
  providers: [
    EmailNotifier,
    SmsNotifier,
    NotificationService,
    {
      // Channel selected from the CHANNEL environment variable.
      provide: NOTIFIER,
      useFactory: (email: EmailNotifier, sms: SmsNotifier) => (process.env.CHANNEL === 'sms' ? sms : email),
      inject: [EmailNotifier, SmsNotifier],
    },
  ],
})
export class AppModule {}
