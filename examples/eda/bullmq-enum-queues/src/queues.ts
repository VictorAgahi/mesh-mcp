// The queue names of the fixture, declared once and imported everywhere.
export enum JobQueue {
  EMAIL = 'email-jobs',
  REPORTS = 'report-jobs',
  AUDIT = 'audit-jobs',
}

// Same member name as JobQueue.EMAIL, another queue: the two must never share a topic.
export enum LegacyQueue {
  EMAIL = 'legacy-email-jobs',
}
