import { Inject, Injectable } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';

export const NOTIFIER = Symbol('NOTIFIER');

export interface Notifier {
  send(orderId: string): void;
}

@Injectable()
export class EmailNotifier implements Notifier {
  constructor(@Inject('KAFKA_CLIENT') private readonly client: ClientKafka) {}

  send(orderId: string) {
    this.client.emit('email.queued', { orderId });
  }
}

@Injectable()
export class SmsNotifier implements Notifier {
  constructor(@Inject('KAFKA_CLIENT') private readonly client: ClientKafka) {}

  send(orderId: string) {
    this.client.emit('sms.queued', { orderId });
  }
}
