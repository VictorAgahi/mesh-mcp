import { connect } from 'nats';

export async function emitTemperature(): Promise<void> {
  const nc = await connect({ servers: 'localhost:4222' });
  nc.publish('sensors.temp', 'x');
}

export async function listenSensors(): Promise<void> {
  const nc = await connect({ servers: 'localhost:4222' });
  nc.subscribe('sensors.*');
}

export async function listenGhosts(): Promise<void> {
  const nc = await connect({ servers: 'localhost:4222' });
  nc.subscribe('ghosts.*');
}
