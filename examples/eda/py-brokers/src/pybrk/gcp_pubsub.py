from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path("proj", "price-alerts")
sub_path = subscriber.subscription_path("proj", "price-alerts-sub")
subscriber.create_subscription(name=sub_path, topic=topic_path)
orphan_path = subscriber.subscription_path("proj", "orphan-sub")


def alert(data):
    publisher.publish(topic_path, data)  # price-alerts


def listen(callback):
    subscriber.subscribe(sub_path, callback=callback)  # price-alerts, through create_subscription


def listen_orphan(callback):
    subscriber.subscribe(orphan_path, callback=callback)
