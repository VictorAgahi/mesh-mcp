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

export async function emitDevice(): Promise<void> {
  const nc = await connect({ servers: 'localhost:4222' });
  nc.publish('devices.created', 'x');
  nc.publish('devices.deleted', 'x');
  nc.publish('firmware.unmatched', 'x');
}

export async function listenDevices(): Promise<void> {
  const nc = await connect({ servers: 'localhost:4222' });
  nc.subscribe('devices.*');
}
