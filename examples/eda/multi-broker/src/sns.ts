import { SNSClient, PublishCommand } from '@aws-sdk/client-sns';

const sns = new SNSClient({ region: 'eu-west-1' });

export async function announceOrder(orderId: string): Promise<void> {
  await sns.send(new PublishCommand({ TopicArn: 'arn:aws:sns:eu-west-1:123456789012:orders', Message: orderId }));
}
