import { Injectable } from '@nestjs/common';
import { StockPublisher } from './stock.publisher';

@Injectable()
export class InventoryService {
  constructor(private readonly stockPublisher: StockPublisher) {}

  async reserve(item: { orderId: string; sku: string }) {
    const reserved = { orderId: item.orderId, sku: item.sku, qty: 1 };
    this.stockPublisher.publishReserved(reserved);
  }
}
