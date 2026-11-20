export class IssueRefundCommand {
  constructor(public readonly orderId: string, public readonly amount: number) {}
}

export class RefundIssuedEvent {
  constructor(public readonly orderId: string, public readonly amount: number) {}
}

export class SendReceiptCommand {
  constructor(public readonly orderId: string) {}
}
