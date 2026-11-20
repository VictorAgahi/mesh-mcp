import { Injectable } from '@nestjs/common';
import { InventoryService } from './inventory.service';

@Injectable()
export class OrderService {
  constructor(private readonly inventory: InventoryService) {}

  async place(dto: { orderId: string; sku: string }) {
    const normalized = { ...dto, sku: dto.sku.trim().toUpperCase() };
    await this.inventory.reserve(normalized);
  }
}
