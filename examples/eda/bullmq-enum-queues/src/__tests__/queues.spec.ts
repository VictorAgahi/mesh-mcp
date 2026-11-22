// A test file declaring its own JobQueue: its values must never name a production queue.
export enum JobQueue {
  EMAIL = 'TEST-ONLY-email',
}
