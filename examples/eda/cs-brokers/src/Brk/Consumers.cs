using MassTransit;

namespace Brk;

public class OrderCreatedConsumer : IConsumer<OrderCreated>
{
    private readonly ISendEndpointProvider _provider;

    public OrderCreatedConsumer(ISendEndpointProvider provider) => _provider = provider;

    public async Task Consume(ConsumeContext<OrderCreated> context)
    {
        await context.Publish(new OrderShipped(context.Message.OrderId));
        var endpoint = await _provider.GetSendEndpoint(new Uri("queue:billing-queue"));
        await endpoint.Send(new BillOrder(context.Message.OrderId));
    }
}

public class PaymentSettledConsumer : IConsumer<PaymentSettled>, IConsumer<Batch<OrderShipped>>
{
    public Task Consume(ConsumeContext<PaymentSettled> context) => Task.CompletedTask;

    public Task Consume(ConsumeContext<Batch<OrderShipped>> context) => Task.CompletedTask;
}
