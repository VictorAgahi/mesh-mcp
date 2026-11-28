import amqp from 'amqplib';

export async function forwardToRabbit(ch: amqp.Channel, orderId: string): Promise<void> {
  ch.sendToQueue('orders', Buffer.from(orderId));
}
