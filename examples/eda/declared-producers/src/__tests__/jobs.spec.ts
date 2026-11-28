import { JobsOutbox } from '../outbox/outbox';
import { JobType, Queue } from '../enums';

it('builds a job', () => {
  JobsOutbox.createJob({ type: JobType.SEND_MAIL, target: Queue.MAIL, payload: {} });
});
