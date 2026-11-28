using Azure.Messaging.ServiceBus;
using MassTransit;

namespace Brk.Legacy;

// A homegrown bus and local classes named like the clients: none of them is a broker call.
public interface IConsumer<T>
{
}

public class IPublishEndpoint
{
    public void Publish(object message)
    {
    }
}

public class ServiceBusClient
{
    public object CreateSender(string queue) => queue;
}

public class LocalHandler : IConsumer<OrderCreated>
{
}

public class EventBus
{
    public void Publish(object message)
    {
    }
}

public class Decoys
{
    private readonly IPublishEndpoint _endpoint;
    private readonly ServiceBusClient _client;
    private readonly EventBus _events;

    public void Run(Func<object, Task> publish, Notifier notifier)
    {
        _endpoint.Publish(new OrderCreated(Guid.Empty));
        _client.CreateSender("decoy.queue");
        _events.Publish(new OrderShipped(Guid.Empty));
        notifier.Publish(new OrderCreated(Guid.Empty));
        Console.WriteLine("Publish(new OrderCreated()) then CreateSender(\"decoy.logged\")");
    }
}
