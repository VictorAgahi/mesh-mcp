import { Kafka } from 'kafkajs';
import amqp from 'amqplib';
import { forwardToRabbit } from './bridge';

const consumer = new Kafka({ brokers: ['localhost:9092'] }).consumer({ groupId: 'billing' });

export async function startBilling(ch: amqp.Channel): Promise<void> {
  await consumer.subscribe({ topic: 'orders' });
  await forwardToRabbit(ch, 'pending');
}
