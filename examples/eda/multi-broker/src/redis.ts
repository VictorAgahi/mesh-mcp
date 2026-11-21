import Redis from 'ioredis';

const redis = new Redis();

export async function appendOrder(orderId: string): Promise<void> {
  await redis.xadd('orders', '*', 'id', orderId);
}

export async function readOrders(): Promise<void> {
  await redis.xread('STREAMS', 'orders', '$');
}

export async function appendEvent(): Promise<void> {
  await redis.xadd('events.created', '*', 'k', 'v');
}
