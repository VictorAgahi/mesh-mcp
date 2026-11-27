import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function emitHub0(): Promise<void> {
  await producer.send({ topic: 'hub.t0', messages: [{ value: 'x' }] });
}

export async function emitHub1(): Promise<void> {
  await producer.send({ topic: 'hub.t1', messages: [{ value: 'x' }] });
}

export async function emitHub2(): Promise<void> {
  await producer.send({ topic: 'hub.t2', messages: [{ value: 'x' }] });
}

export async function emitHub3(): Promise<void> {
  await producer.send({ topic: 'hub.t3', messages: [{ value: 'x' }] });
}

export async function emitHub4(): Promise<void> {
  await producer.send({ topic: 'hub.t4', messages: [{ value: 'x' }] });
}

export async function emitHub5(): Promise<void> {
  await producer.send({ topic: 'hub.t5', messages: [{ value: 'x' }] });
}

