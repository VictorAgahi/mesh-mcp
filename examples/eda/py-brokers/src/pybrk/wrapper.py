import pika
from confluent_kafka import Producer


class KafkaBus:
    def __init__(self):
        self.producer = Producer({"bootstrap.servers": "kafka:9092"})

    def publish(self, topic, value):
        self.producer.produce(topic, value)

    def emit(self, value):
        self.producer.produce("wrap.topic", value)


class RabbitBus:
    def __init__(self):
        self.channel = pika.BlockingConnection(pika.ConnectionParameters("localhost")).channel()

    def publish(self, queue, body):
        self.channel.basic_publish(exchange="", routing_key=queue, body=body)


kafka_bus = KafkaBus()
rabbit_bus = RabbitBus()


def run():
    kafka_bus.publish("bus.topic", 1)
    kafka_bus.emit(2)
    rabbit_bus.publish("rabbit.wrapped", b"x")
