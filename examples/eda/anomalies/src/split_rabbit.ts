import amqp from 'amqplib';

export async function startPayments(ch: amqp.Channel): Promise<void> {
  await ch.consume('payments', (msg) => {});
}
