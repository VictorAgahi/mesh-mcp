from confluent_kafka import Producer

from layered_py.consumer import OrderConsumer
from layered_py.handler import OrderHandler
from layered_py.publisher import InvoicePublisher
from layered_py.service import InvoiceService


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    service = InvoiceService(InvoicePublisher(producer))
    OrderConsumer(OrderHandler(service)).run()


if __name__ == "__main__":
    main()
