import { Kafka } from 'kafkajs';
import { publishLineC } from './line_next';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function startLine(): Promise<void> {
  await producer.send({ topic: 'line.a', messages: [{ value: 'x' }] });
}

export async function onLineB(): Promise<void> {
  await consumer.subscribe({ topic: 'line.b' });
  await publishLineC();
}

