class OrderHandler:
    def __init__(self, service):
        self._service = service

    def handle(self, message):
        order_id, _, total = message.value().decode().partition(":")
        self._service.issue(order_id, float(total or 0))
