import redis
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "kafka:9092"})
bus = redis.Redis()
feed = bus.pubsub()


def settle(value):
    producer.produce("payments.settled", value)


def watch():
    feed.subscribe("payments.settled")
