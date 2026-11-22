import os

import pika

ORDERS_QUEUE = "orders"
AUDIT_QUEUE = "audit.events"

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()
channel.queue_declare(queue=ORDERS_QUEUE)


def publish_order(body):
    channel.basic_publish(exchange="", routing_key=ORDERS_QUEUE, body=body)  # orders


def publish_created(body):
    channel.basic_publish("events", "order.created", body)


def publish_pattern(body):
    channel.basic_publish(exchange="events", routing_key="order.*", body=body)


def publish_dynamic(queue_name, body):
    channel.basic_publish(exchange="", routing_key=queue_name, body=body)


def publish_payment(body):
    queue = os.getenv("PAYMENTS_QUEUE", "payments.fallback")
    channel.basic_publish(exchange="", routing_key=queue, body=body)


def consume_orders(on_message):
    channel.basic_consume(queue=ORDERS_QUEUE, on_message_callback=on_message)  # orders
    channel.basic_consume(AUDIT_QUEUE, on_message)  # audit.events


def dead_letters(on_message):
    dead = os.environ["ORDERS_DEAD_LETTER"]
    channel.basic_get(queue=dead)
