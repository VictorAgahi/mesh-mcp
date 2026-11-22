import nats


async def run(on_price):
    nc = await nats.connect("nats://localhost")
    js = nc.jetstream()
    await nc.publish("audit.events", b"x")
    await nc.publish("telemetry.*", b"x")
    await nc.subscribe("price.updates", cb=on_price)
    await js.publish("orders.shipped", b"y")
    await js.pull_subscribe("orders.shipped", durable="worker")
    return await nc.request("rpc.quote", b"")
