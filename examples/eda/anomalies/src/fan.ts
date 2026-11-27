import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function emitFanWide(): Promise<void> {
  await producer.send({ topic: 'fan.wide', messages: [{ value: 'x' }] });
}

export async function emitFanNarrow(): Promise<void> {
  await producer.send({ topic: 'fan.narrow', messages: [{ value: 'x' }] });
}

export async function wideConsumer0(): Promise<void> {
  await consumer.subscribe({ topic: 'fan.wide' });
}

export async function wideConsumer1(): Promise<void> {
  await consumer.subscribe({ topic: 'fan.wide' });
}

export async function wideConsumer2(): Promise<void> {
  await consumer.subscribe({ topic: 'fan.wide' });
}

export async function wideConsumer3(): Promise<void> {
  await consumer.subscribe({ topic: 'fan.wide' });
}

export async function wideConsumer4(): Promise<void> {
  await consumer.subscribe({ topic: 'fan.wide' });
}

export async function wideConsumer5(): Promise<void> {
  await consumer.subscribe({ topic: 'fan.wide' });
}

export async function narrowConsumer0(): Promise<void> {
  await consumer.subscribe({ topic: 'fan.narrow' });
}

export async function narrowConsumer1(): Promise<void> {
  await consumer.subscribe({ topic: 'fan.narrow' });
}

export async function narrowConsumer2(): Promise<void> {
  await consumer.subscribe({ topic: 'fan.narrow' });
}

export async function narrowConsumer3(): Promise<void> {
  await consumer.subscribe({ topic: 'fan.narrow' });
}

export async function narrowConsumer4(): Promise<void> {
  await consumer.subscribe({ topic: 'fan.narrow' });
}

