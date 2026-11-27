import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function emitAudit(): Promise<void> {
  await producer.send({ topic: 'audit.created', messages: [{ value: 'x' }] });
}

export async function emitBilling(): Promise<void> {
  await producer.send({ topic: 'billing.paid', messages: [{ value: 'x' }] });
}

