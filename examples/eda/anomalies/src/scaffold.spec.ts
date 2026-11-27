import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'anomalies', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'anomalies' });

export async function scaffoldProducer(): Promise<void> {
  await producer.send({ topic: 'scaffold.created', messages: [{ value: 'x' }] });
}

export async function scaffoldConsumer(): Promise<void> {
  await consumer.subscribe({ topic: 'scaffold.created' });
}

