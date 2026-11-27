import { Kafka } from 'kafkajs';
import { publishLoopC } from './loop_c';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function onLoopB(): Promise<void> {
  await consumer.subscribe({ topic: 'loop.b' });
  await publishLoopC();
}

export async function publishLoopB(): Promise<void> {
  await producer.send({ topic: 'loop.b', messages: [{ value: 'x' }] });
}

