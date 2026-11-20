class InvoicePublisher:
    def __init__(self, producer):
        self._producer = producer

    def announce(self, order_id, total):
        payload = f"{order_id}:{total}".encode()
        self._producer.produce("invoice.issued", value=payload)
