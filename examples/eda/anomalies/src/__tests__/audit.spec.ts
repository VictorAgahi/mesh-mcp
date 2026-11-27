import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function auditContract(): Promise<void> {
  await consumer.subscribe({ topic: 'audit.created' });
}

export async function billingContract(): Promise<void> {
  await consumer.subscribe({ topic: 'billing.paid' });
}

