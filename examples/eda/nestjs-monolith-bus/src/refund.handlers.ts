import { CommandHandler, EventsHandler, ICommandHandler, IEventHandler } from '@nestjs/cqrs';
import { IssueRefundCommand, RefundIssuedEvent, SendReceiptCommand } from './refund.messages';
import { RefundLedger } from './refund.ledger';

@CommandHandler(IssueRefundCommand)
export class IssueRefundHandler implements ICommandHandler<IssueRefundCommand> {
  constructor(private readonly ledger: RefundLedger) {}

  async execute(command: IssueRefundCommand) {
    this.ledger.record(command.orderId, command.amount);
  }
}

@CommandHandler(SendReceiptCommand)
export class SendReceiptHandler implements ICommandHandler<SendReceiptCommand> {
  async execute(command: SendReceiptCommand) {
    console.error(`receipt sent for ${command.orderId}`);
  }
}

@EventsHandler(RefundIssuedEvent)
export class RefundAuditHandler implements IEventHandler<RefundIssuedEvent> {
  handle(event: RefundIssuedEvent) {
    console.error(`audit: refund of ${event.amount} for ${event.orderId}`);
  }
}
