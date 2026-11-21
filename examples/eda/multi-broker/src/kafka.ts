import { Kafka } from 'kafkajs';
import { billOrder } from './billing';

const kafka = new Kafka({ clientId: 'shop', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'billing' });

export async function publishOrderPlaced(orderId: string): Promise<void> {
  await producer.send({ topic: 'orders', messages: [{ key: orderId, value: orderId }] });
}

export async function startBilling(): Promise<void> {
  await consumer.subscribe({ topic: 'orders' });
  await billOrder('pending');
}
