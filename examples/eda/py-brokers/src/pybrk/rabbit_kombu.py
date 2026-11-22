from kombu import Connection, Consumer, Exchange, Queue

shipping_exchange = Exchange("shipping", type="direct")
shipping_queue = Queue("shipping.requests", shipping_exchange, routing_key="shipping.requests")
conn = Connection("amqp://localhost")


def send(payload):
    with conn.Producer() as producer:
        producer.publish(payload, routing_key="shipping.requests", exchange=shipping_exchange)


def listen(callback):
    with Consumer(conn, queues=[shipping_queue], callbacks=[callback]):  # shipping.requests
        conn.drain_events()
