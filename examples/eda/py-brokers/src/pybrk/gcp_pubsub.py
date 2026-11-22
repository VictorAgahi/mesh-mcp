from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path("proj", "price-alerts")
sub_path = subscriber.subscription_path("proj", "price-alerts-sub")


def alert(data):
    publisher.publish(topic_path, data)  # price-alerts


def listen(callback):
    subscriber.subscribe(sub_path, callback=callback)  # price-alerts-sub
