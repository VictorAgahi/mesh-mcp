import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function refreshCatalog(): Promise<void> {
  await producer.send({ topic: 'catalog.refreshed', messages: [{ value: 'x' }] });
}

export async function listenLegacyImport(): Promise<void> {
  await consumer.subscribe({ topic: 'legacy.imported' });
}

