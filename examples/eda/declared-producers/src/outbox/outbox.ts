// The declared helpers themselves: a declaration is not a call.
export class JobsOutbox {
  static createJob(props: { type: string; target: string; payload: object; emitter?: string }) {
    return props;
  }
}

export class EventOutbox {
  static createEvent(props: { type: string; targetServices: string[] }) {
    return props;
  }
}
