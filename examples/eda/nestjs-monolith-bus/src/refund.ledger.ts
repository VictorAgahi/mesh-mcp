import { Injectable } from '@nestjs/common';
import { EventBus } from '@nestjs/cqrs';
import { RefundIssuedEvent } from './refund.messages';

@Injectable()
export class RefundLedger {
  constructor(private readonly eventBus: EventBus) {}

  record(orderId: string, amount: number) {
    this.eventBus.publish(new RefundIssuedEvent(orderId, amount));
  }
}
