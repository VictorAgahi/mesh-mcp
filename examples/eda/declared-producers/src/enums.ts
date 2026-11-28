export enum JobType {
  SCAN_FILE = 'scan_file',
  SEND_MAIL = 'send_mail',
}

export enum Queue {
  STORAGE = 'storage-queue',
  MAIL = 'mail-queue',
}

export enum Streams {
  USER_CREATED = 'user:created',
  USER_DELETED = 'user:deleted',
}

export enum EventType {
  USER_REGISTERED = 'user_registered_event',
}
