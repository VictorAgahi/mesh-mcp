import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function onLineA(): Promise<void> {
  await consumer.subscribe({ topic: 'line.a' });
  await publishLineB();
}

export async function publishLineB(): Promise<void> {
  await producer.send({ topic: 'line.b', messages: [{ value: 'x' }] });
}

export async function publishLineC(): Promise<void> {
  await producer.send({ topic: 'line.c', messages: [{ value: 'x' }] });
}

export async function onLineC(): Promise<void> {
  await consumer.subscribe({ topic: 'line.c' });
}

