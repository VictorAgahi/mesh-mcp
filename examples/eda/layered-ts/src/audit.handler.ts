import { Controller, Inject } from '@nestjs/common';
import { ClientKafka, EventPattern, Payload } from '@nestjs/microservices';

// Audit trail handler.
@Controller()
export class AuditHandler {
  constructor(@Inject('KAFKA_CLIENT') private readonly client: ClientKafka) {}

  @EventPattern('audit.requested')
  async onAudit(@Payload() req: { subject: string }) {
    this.client.emit('audit.completed', { subject: req.subject, ok: true });
  }
}
