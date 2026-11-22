from confluent_kafka import Consumer
from kafka import KafkaConsumer, KafkaProducer

clicks = KafkaConsumer("clicks.raw", "clicks.late", bootstrap_servers="kafka:9092")
producer = KafkaProducer(bootstrap_servers="kafka:9092")
views = Consumer({"bootstrap.servers": "kafka:9092", "group.id": "views"})


def emit(value):
    producer.send("clicks.clean", value)


def watch():
    views.subscribe(topics=["views.raw"])
