import logging

import redis

logger = logging.getLogger(__name__)
r = redis.Redis()


class Bus:
    def publish(self, topic, message):
        logger.info("published %s", message)

    def subscribe(self, topic):
        return [topic]


bus = Bus()


def local_bus():
    bus.publish("decoy.local", {"a": 1})
    bus.subscribe("decoy.local")


def log_lines():
    logger.info("will publish to decoy.logged then subscribe to decoy.logged")
    print("basic_publish routing_key=decoy.printed")


def dynamic(channel_name, client):
    r.publish(channel_name, "x")
    client.publish(channel_name, "x")
    r.publish(f"decoy.{channel_name}", "x")
    r.publish("decoy." + channel_name, "x")


def no_arguments():
    r.publish()
