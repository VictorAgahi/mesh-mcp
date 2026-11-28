import { EventPattern } from '@nestjs/microservices';
import { Streams } from './enums';

export class UserHandler {
  @EventPattern(Streams.USER_CREATED)
  onCreated() {}
}
