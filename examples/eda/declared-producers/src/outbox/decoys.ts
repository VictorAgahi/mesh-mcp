import { JobType, Queue } from '../enums';

// Same method name on another receiver: not the declared helper.
declare const OtherOutbox: { createJob(props: object): void };
export function otherReceiver() {
  OtherOutbox.createJob({ type: JobType.SEND_MAIL, target: Queue.MAIL });
}

// A parameter named like the declared class hides it.
export function shadowed(JobsOutbox: { createJob(props: object): void }) {
  JobsOutbox.createJob({ type: JobType.SCAN_FILE, target: Queue.MAIL });
}

// The call written in a comment or a string is not a call.
// JobsOutbox.createJob({ type: JobType.SEND_MAIL, target: Queue.MAIL });
export const hint = "JobsOutbox.createJob({ type: JobType.SEND_MAIL, target: Queue.MAIL })";

// An import alias of the declared class is not followed (known limit, nothing is guessed).
import { JobsOutbox as AliasedOutbox } from './outbox';
export function aliased() {
  return AliasedOutbox.createJob({
    type: 'aliased-job',
    target: Queue.MAIL,
    payload: {},
  });
}
