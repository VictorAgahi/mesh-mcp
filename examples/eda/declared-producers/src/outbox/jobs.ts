import { JobsOutbox } from './outbox';
import { JobType, Queue } from '../enums';

export function buildScanFileJob(fileId: string) {
  return JobsOutbox.createJob({
    type: JobType.SCAN_FILE,
    emitter: 'storage',
    target: Queue.STORAGE,
    payload: { fileId },
  });
}

export function buildMailJob(mailId: string) {
  return JobsOutbox.createJob({ type: JobType.SEND_MAIL, target: Queue.MAIL, payload: { mailId } });
}

// The type is a parameter, so it is not a name: only the target is one.
export function buildJob(jobType: string, payload: object) {
  return JobsOutbox.createJob({
    type: jobType,
    emitter: 'storage',
    payload,
    // the queue every storage job goes to
    target: Queue.STORAGE,
  });
}
