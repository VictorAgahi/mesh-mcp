import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';

interface Base {
  id: string;
}

// Declared here, fields not statically known: not compared, never src/order.ts's Evt.
export type Evt = Base & { x: number };

@Controller()
export class EvtConsumer {
  @EventPattern('evt.fired')
  onFired(@Payload() e: Evt) {
    return e.x;
  }
}
