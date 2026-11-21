import { connect } from 'nats';

export async function announce(orderId: string): Promise<void> {
  const nc = await connect({ servers: 'localhost:4222' });
  nc.publish('orders', orderId);
}

export async function listen(): Promise<void> {
  const nc = await connect({ servers: 'localhost:4222' });
  nc.subscribe('orders');
}

export async function listenEvents(): Promise<void> {
  const nc = await connect({ servers: 'localhost:4222' });
  nc.subscribe('events.*');
}
