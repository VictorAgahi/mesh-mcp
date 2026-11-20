from confluent_kafka import Producer

_producer = Producer({"bootstrap.servers": "localhost:9092"})


def build_report(order_id):
    body = f"report for {order_id}".encode()
    _producer.produce("report.ready", value=body)
