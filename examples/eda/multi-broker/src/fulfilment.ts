import amqp from 'amqplib';

export async function fulfilOrder(ch: amqp.Channel, orderId: string): Promise<void> {
  ch.sendToQueue('shipments', Buffer.from(orderId));
}
