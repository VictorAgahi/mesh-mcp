import { Body, Controller, Post } from '@nestjs/common';
import { RefundService } from './refund.service';

@Controller('refunds')
export class RefundController {
  constructor(private readonly refunds: RefundService) {}

  @Post()
  request(@Body() dto: { orderId: string; amount: number }) {
    return this.refunds.request(dto.orderId, dto.amount);
  }
}
