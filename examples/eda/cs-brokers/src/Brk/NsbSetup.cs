using NServiceBus;

namespace Brk;

public static class NsbSetup
{
    public static EndpointConfiguration Configure()
    {
        var endpoint = new EndpointConfiguration("Sales");
        var transport = endpoint.UseTransport<AzureServiceBusTransport>();
        return endpoint;
    }
}

public class OrderPlacedHandler : IHandleMessages<OrderPlaced>
{
    public Task Handle(OrderPlaced message, IMessageHandlerContext context) => context.Reply(new BillOrder(message.OrderId));
}
