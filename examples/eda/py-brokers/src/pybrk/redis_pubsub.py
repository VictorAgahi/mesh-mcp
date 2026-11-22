import redis

r = redis.Redis(host="localhost")
pubsub = r.pubsub()


def notify(message):
    r.publish("alerts.fired", message)


def listen(on_beat):
    pubsub.subscribe("alerts.fired", "metrics.cpu")
    pubsub.psubscribe("logs.*")
    pubsub.subscribe(heartbeat=on_beat)
