using MassTransit;
using Microsoft.Extensions.DependencyInjection;

namespace Brk;

public static class MassTransitSetup
{
    public static IServiceCollection AddBus(this IServiceCollection services)
    {
        services.AddMassTransit(x =>
        {
            x.AddConsumer<InvoiceConsumer>();
            x.UsingRabbitMq((ctx, cfg) =>
            {
                cfg.Host("rabbit.internal");
                cfg.ReceiveEndpoint("orders", e => e.ConfigureConsumer<InvoiceConsumer>(ctx));
                cfg.ReceiveEndpoint("audit-queue", e => { });
                cfg.ReceiveEndpoint(e => { });
            });
        });
        return services;
    }
}

public class InvoicePublisher
{
    private readonly IPublishEndpoint _bus;

    public InvoicePublisher(IPublishEndpoint bus) => _bus = bus;

    public Task Issue(Guid id) => _bus.Publish(new InvoiceIssued(id));
}

public class InvoiceConsumer : IConsumer<InvoiceIssued>
{
    public Task Consume(ConsumeContext<InvoiceIssued> context) => Task.CompletedTask;
}
