using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;

namespace Brk;

public class SdkWorker
{
    private readonly ServiceBusClient _client;
    private readonly IConfiguration _config;

    public SdkWorker(ServiceBusClient client, IConfiguration config)
    {
        _client = client;
        _config = config;
    }

    public async Task Run()
    {
        var sender = _client.CreateSender("orders");
        var notify = _client.CreateSender("notifications");
        var processor = _client.CreateProcessor("invoices", new ServiceBusProcessorOptions());
        var topic = _client.CreateProcessor("billing", "billing-sub");
        var receiver = _client.CreateReceiver("dead-letters");
        var configured = _client.CreateSender(_config["Billing:Topic"] ?? "billing.fallback");
        var refunds = _client.CreateSender(Environment.GetEnvironmentVariable("REFUND_QUEUE"));
        await sender.SendMessageAsync(new ServiceBusMessage("x"));
    }
}
