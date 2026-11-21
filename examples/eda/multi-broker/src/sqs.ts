import { SQSClient, ReceiveMessageCommand } from '@aws-sdk/client-sqs';

const sqs = new SQSClient({ region: 'eu-west-1' });

export async function pollOrders(): Promise<void> {
  await sqs.send(new ReceiveMessageCommand({ QueueUrl: 'https://sqs.eu-west-1.amazonaws.com/123456789012/orders' }));
}
