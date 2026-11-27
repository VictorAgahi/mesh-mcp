import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function announcePayment(): Promise<void> {
  await producer.send({ topic: 'payments', messages: [{ value: 'x' }] });
}

