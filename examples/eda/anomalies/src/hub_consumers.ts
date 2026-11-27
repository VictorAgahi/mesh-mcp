import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function hubConsumer0(): Promise<void> {
  await consumer.subscribe({ topic: 'hub.t0' });
}

export async function hubConsumer1(): Promise<void> {
  await consumer.subscribe({ topic: 'hub.t1' });
}

export async function hubConsumer2(): Promise<void> {
  await consumer.subscribe({ topic: 'hub.t2' });
}

export async function hubConsumer3(): Promise<void> {
  await consumer.subscribe({ topic: 'hub.t3' });
}

export async function hubConsumer4(): Promise<void> {
  await consumer.subscribe({ topic: 'hub.t4' });
}

export async function hubConsumer5(): Promise<void> {
  await consumer.subscribe({ topic: 'hub.t5' });
}

export async function smallConsumer0(): Promise<void> {
  await consumer.subscribe({ topic: 'small.t0' });
}

export async function smallConsumer1(): Promise<void> {
  await consumer.subscribe({ topic: 'small.t1' });
}

export async function smallConsumer2(): Promise<void> {
  await consumer.subscribe({ topic: 'small.t2' });
}

export async function smallConsumer3(): Promise<void> {
  await consumer.subscribe({ topic: 'small.t3' });
}

export async function smallConsumer4(): Promise<void> {
  await consumer.subscribe({ topic: 'small.t4' });
}

