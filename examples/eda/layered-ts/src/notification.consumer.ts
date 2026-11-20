import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { NotificationService } from './notification.service';

@Controller()
export class NotificationConsumer {
  constructor(private readonly notifications: NotificationService) {}

  @EventPattern('shipment.scheduled')
  async onShipmentScheduled(@Payload() event: { orderId: string }) {
    this.notifications.dispatch(event.orderId);
  }
}
