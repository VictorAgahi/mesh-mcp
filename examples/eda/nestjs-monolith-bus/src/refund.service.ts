import { Injectable } from '@nestjs/common';
import { CommandBus } from '@nestjs/cqrs';
import { IssueRefundCommand } from './refund.messages';

@Injectable()
export class RefundService {
  constructor(private readonly commandBus: CommandBus) {}

  request(orderId: string, amount: number) {
    return this.commandBus.execute(new IssueRefundCommand(orderId, amount));
  }
}
