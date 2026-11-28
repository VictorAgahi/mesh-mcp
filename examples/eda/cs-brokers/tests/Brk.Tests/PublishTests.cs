using MassTransit;

namespace Brk.Tests;

public class PublishTests
{
    private readonly IPublishEndpoint _bus;

    public Task Publishes() => _bus.Publish(new OrderCreated(Guid.Empty));
}
