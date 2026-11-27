import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function emitConfigured(): Promise<void> {
  await producer.send({ topic: process.env.PLACEHOLDER_ONLY_TOPIC, messages: [{ value: 'x' }] });
}

export async function listenInbound(): Promise<void> {
  await consumer.subscribe({ topic: process.env.INBOUND_TOPIC });
}

export async function emitShared(): Promise<void> {
  await producer.send({ topic: process.env.SHARED_TOPIC, messages: [{ value: 'x' }] });
}

export async function listenShared(): Promise<void> {
  await consumer.subscribe({ topic: process.env.SHARED_TOPIC });
}

