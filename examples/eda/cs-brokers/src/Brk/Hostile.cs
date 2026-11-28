using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;

namespace Brk;

public class Hostile
{
    private const string Connection = "Endpoint=sb://brk.example.net/;SharedAccessKeyName=root;SharedAccessKey=notarealkey";
    private readonly ServiceBusClient _client;

    public void Run(IConfiguration configuration, string name)
    {
        _client.CreateSender(Connection);
        _client.CreateSender("orders.*");
        _client.CreateSender(configuration["ServiceBus:ConnectionString"]);
        _client.CreateSender(name);
        _client.CreateSender(configuration.GetConnectionString("bus"));
        _client.CreateProcessor("hostile.ok");
    }
}
