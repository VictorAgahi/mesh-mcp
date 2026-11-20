import { Injectable } from '@nestjs/common';
import { ICommand, ofType, Saga } from '@nestjs/cqrs';
import { map, Observable } from 'rxjs';
import { RefundIssuedEvent, SendReceiptCommand } from './refund.messages';

@Injectable()
export class RefundSagas {
  @Saga()
  receiptAfterRefund = (events$: Observable<any>): Observable<ICommand> =>
    events$.pipe(
      ofType(RefundIssuedEvent),
      map((event) => new SendReceiptCommand(event.orderId)),
    );
}
