import { Injectable } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { Refund } from './refund';
import Shipment from './shipment';
import { Quoted } from './quoted';

interface Placed {
  id: string;
  sku: string;
}

interface EvtSent {
  id: string;
  x: number;
}

@Injectable()
export class Publisher {
  constructor(private readonly client: ClientProxy) {}

  issued(r: Refund) {
    this.client.emit('refund.issued', r);
  }

  voided(r: Refund) {
    this.client.emit('refund.voided', r);
  }

  placed(p: Placed) {
    this.client.emit('order2.placed', p);
  }

  fired(e: EvtSent) {
    this.client.emit('evt.fired', e);
  }

  shipped(s: Shipment) {
    this.client.emit('shipment.sent', s);
  }

  quoted(q: Quoted) {
    this.client.emit('quoted.keys', q);
  }
}
