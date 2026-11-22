import { Queue } from 'bullmq';

// Names built at runtime: no constant says what they are, so no topic is registered.
export function tenantQueue(tenant: string): Queue {
  return new Queue(`${tenant}-jobs`);
}

export function namedQueue(name: string): Queue {
  return new Queue(name);
}
