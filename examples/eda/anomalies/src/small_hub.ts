import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function emitSmall0(): Promise<void> {
  await producer.send({ topic: 'small.t0', messages: [{ value: 'x' }] });
}

export async function emitSmall1(): Promise<void> {
  await producer.send({ topic: 'small.t1', messages: [{ value: 'x' }] });
}

export async function emitSmall2(): Promise<void> {
  await producer.send({ topic: 'small.t2', messages: [{ value: 'x' }] });
}

export async function emitSmall3(): Promise<void> {
  await producer.send({ topic: 'small.t3', messages: [{ value: 'x' }] });
}

export async function emitSmall4(): Promise<void> {
  await producer.send({ topic: 'small.t4', messages: [{ value: 'x' }] });
}

