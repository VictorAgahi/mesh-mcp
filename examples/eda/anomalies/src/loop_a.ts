import { Kafka } from 'kafkajs';
import { publishLoopB } from './loop_b';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function onLoopA(): Promise<void> {
  await consumer.subscribe({ topic: 'loop.a' });
  await publishLoopB();
}

export async function publishLoopA(): Promise<void> {
  await producer.send({ topic: 'loop.a', messages: [{ value: 'x' }] });
}

