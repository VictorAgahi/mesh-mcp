import { Inject, Injectable } from '@nestjs/common';
import { NOTIFIER, Notifier } from './notifiers';

@Injectable()
export class NotificationService {
  constructor(@Inject(NOTIFIER) private readonly notifier: Notifier) {}

  dispatch(orderId: string) {
    this.notifier.send(orderId);
  }
}
