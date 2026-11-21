import amqp from 'amqplib';
import { fulfilOrder } from './fulfilment';

export async function enqueueOrder(ch: amqp.Channel, orderId: string): Promise<void> {
  ch.sendToQueue('orders', Buffer.from(orderId));
}

export async function startFulfilment(ch: amqp.Channel): Promise<void> {
  await ch.consume('orders', (msg) => {});
  await fulfilOrder(ch, 'pending');
}
