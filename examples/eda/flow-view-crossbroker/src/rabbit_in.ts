import amqp from 'amqplib';

export async function startFulfil(ch: amqp.Channel): Promise<void> {
  await ch.consume('orders', (msg) => {});
}
