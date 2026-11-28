using MassTransit;

namespace Brk;

public class OrderService
{
    private readonly IPublishEndpoint _bus;
    private readonly IBus _rawBus;

    public OrderService(IPublishEndpoint bus, IBus rawBus)
    {
        _bus = bus;
        _rawBus = rawBus;
    }

    public async Task Create(Guid id, PaymentSettled settled)
    {
        await _bus.Publish(new OrderCreated(id));
        await _rawBus.Publish<PaymentSettled>(settled);
        await _bus.Publish<OrderCreated>(new { OrderId = id });
    }
}
