import { JobsOutbox } from './outbox';
import { LegacyQueue } from '../legacy-enums';

// LegacyQueue.STORAGE is another queue than Queue.STORAGE, whatever the member's name.
export function legacyCleanup() {
  return JobsOutbox.createJob({
    type: 'legacy-cleanup',
    target: LegacyQueue.STORAGE,
    payload: {},
  });
}
