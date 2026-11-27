import { Kafka } from 'kafkajs';
import { publishLoopA } from './loop_a';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function onLoopC(): Promise<void> {
  await consumer.subscribe({ topic: 'loop.c' });
  await publishLoopA();
}

export async function publishLoopC(): Promise<void> {
  await producer.send({ topic: 'loop.c', messages: [{ value: 'x' }] });
}

