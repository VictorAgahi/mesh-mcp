using NServiceBus;

namespace Brk;

public class PlaceOrderHandler : IHandleMessages<PlaceOrder>
{
    public async Task Handle(PlaceOrder message, IMessageHandlerContext context)
    {
        await context.Publish(new OrderPlaced(message.OrderId));
        await context.Send("billing", new BillOrder(message.OrderId));
    }
}

public class Starter
{
    private readonly IMessageSession _session;

    public Starter(IMessageSession session) => _session = session;

    public Task Start(Guid id) => _session.SendLocal(new PlaceOrder(id));
}
