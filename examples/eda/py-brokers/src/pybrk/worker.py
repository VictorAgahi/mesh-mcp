import pika


class Worker:
    queue_name = "tasks.new"

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        self.channel = self.connection.channel()

    def push(self, body):
        self.channel.basic_publish(exchange="", routing_key=self.queue_name, body=body)  # tasks.new

    def pull(self):
        return self.channel.basic_get(queue="tasks.new")


def drain(ch: pika.adapters.blocking_connection.BlockingChannel, on_message):
    ch.basic_consume(queue="tasks.drained", on_message_callback=on_message)
