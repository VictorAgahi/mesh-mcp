# import redis
# import pika
import boto3
import nats

sqs = boto3.client("sqs")


class Sink:
    def send_message(self, QueueUrl, MessageBody):
        return QueueUrl


def shadow_param(sqs):
    sqs.send_message(QueueUrl="https://sqs.eu-west-1.amazonaws.com/1/shadow.param", MessageBody="x")


def shadow_comprehension(sinks):
    return [sqs.send_message(QueueUrl="https://sqs.eu-west-1.amazonaws.com/1/shadow.comp", MessageBody="x") for sqs in sinks]


def shadow_lambda(sinks):
    return list(map(lambda sqs: sqs.send_message(QueueUrl="https://sqs.eu-west-1.amazonaws.com/1/shadow.lambda", MessageBody="x"), sinks))


def shadow_loop(sinks):
    for sqs in sinks:
        sqs.send_message(QueueUrl="https://sqs.eu-west-1.amazonaws.com/1/shadow.loop", MessageBody="x")


def shadow_local_class():
    sqs = Sink()
    sqs.send_message(QueueUrl="https://sqs.eu-west-1.amazonaws.com/1/shadow.class", MessageBody="x")


async def not_imported(url):
    nc = await nats.connect(url)
    pika_channel = pika.BlockingConnection(url).channel()
    pika_channel.basic_publish(exchange="", routing_key="shadow.pika", body=b"x")
    await nc.publish("shadow.nats.real", b"x")
