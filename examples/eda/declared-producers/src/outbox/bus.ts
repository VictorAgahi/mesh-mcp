import { Streams } from '../enums';

declare const streamBus: { publish(stream: string, event: object): void };
declare const mailOutbox: { enqueue(topic: string, payload: object): void };
declare const audit: { enqueue(topic: string, payload: object): void };

export function announce(userId: string) {
  streamBus.publish(Streams.USER_CREATED, { userId });
  mailOutbox.enqueue('welcome-mail', { userId });
}

// Not the declared receivers: a logger-like object with the same method names.
export function noise(userId: string) {
  audit.enqueue('audit-trail', { userId });
}
