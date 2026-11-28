import { Kafka } from 'kafkajs';

const producer = new Kafka({ brokers: ['localhost:9092'] }).producer();

export async function publishOrder(orderId: string): Promise<void> {
  await producer.send({ topic: 'orders', messages: [{ value: orderId }] });
}
