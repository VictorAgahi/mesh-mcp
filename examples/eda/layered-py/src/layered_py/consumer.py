from confluent_kafka import Consumer


class OrderConsumer:
    def __init__(self, handler):
        self._handler = handler
        self._consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "invoicing"})

    def run(self):
        self._consumer.subscribe(["orders.created"])
        while True:
            msg = self._consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            self._handler.handle(msg)
