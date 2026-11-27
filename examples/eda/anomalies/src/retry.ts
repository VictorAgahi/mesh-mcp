import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function onJobRun(): Promise<void> {
  await consumer.subscribe({ topic: 'jobs.run' });
  await requeueJob();
}

export async function requeueJob(): Promise<void> {
  await producer.send({ topic: 'jobs.run', messages: [{ value: 'x' }] });
}

export async function startJob(): Promise<void> {
  await producer.send({ topic: 'jobs.run', messages: [{ value: 'x' }] });
}

