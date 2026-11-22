import aio_pika


async def start(url, handle):
    connection = await aio_pika.connect_robust(url)
    channel = await connection.channel()
    queue = await channel.declare_queue("invoices.todo", durable=True)
    await queue.consume(handle)  # invoices.todo
    retries = await channel.declare_queue(name="invoices.retry")
    async with retries.iterator() as messages:  # invoices.retry
        async for message in messages:
            print(message.body)
    await channel.default_exchange.publish(aio_pika.Message(body=b"done"), routing_key="invoices.done")
    exchange = await channel.declare_exchange("events", aio_pika.ExchangeType.TOPIC)
    await exchange.publish(aio_pika.Message(body=b"paid"), routing_key="invoice.paid")
