from layered_py.tasks import generate_report


class InvoiceService:
    def __init__(self, publisher):
        self._publisher = publisher

    def issue(self, order_id, total):
        self._publisher.announce(order_id, total)
        generate_report.delay(order_id)
