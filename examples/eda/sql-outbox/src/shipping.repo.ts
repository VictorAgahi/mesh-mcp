import { Pool } from 'pg';

export class ShippingRepo {
  constructor(private readonly pool: Pool) {}

  async markShipped(orderId: string): Promise<void> {
    // INSERT INTO outbox (topic, payload) VALUES ('comment.only', 1) in a comment is not a producer.
    await this.pool.query(
      `INSERT INTO outbox (topic, payload)
       VALUES ('order.shipped', $1)`,
      [JSON.stringify({ orderId })],
    );
  }

  async publish(topic: string, body: string): Promise<void> {
    await this.pool.query('INSERT INTO outbox (topic, payload) VALUES ($1, $2)', [topic, body]);
  }

  async audit(message: string): Promise<void> {
    await this.pool.query("INSERT INTO audit_log (topic) VALUES ('audit.only')", [message]);
  }

  hint(): string {
    return "to publish, run INSERT INTO outbox (topic, payload) VALUES ('prose.only', 1)";
  }
}
