import { Controller } from '@nestjs/common';
import { EventPattern } from '@nestjs/microservices';

@Controller()
export class AuditConsumer {
  @EventPattern('invoice.issued')
  async record(dto: { invoiceId: string }) {
    console.log(dto.invoiceId);
  }
}
