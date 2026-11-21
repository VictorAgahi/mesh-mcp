import { Kafka } from 'kafkajs';

const producer = new Kafka({ brokers: ['localhost:9092'] }).producer();

export async function billOrder(orderId: string): Promise<void> {
  await producer.send({ topic: 'invoices', messages: [{ value: orderId }] });
}
