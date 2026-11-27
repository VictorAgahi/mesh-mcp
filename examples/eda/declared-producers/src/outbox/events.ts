import { EventOutbox } from './outbox';
import { EventType, Streams } from '../enums';

export function userCreated(userId: string) {
  return EventOutbox.createEvent({
    type: EventType.USER_REGISTERED,
    userId,
    targetServices: [
      Streams.USER_CREATED,
      Streams.USER_DELETED,
    ],
  });
}
