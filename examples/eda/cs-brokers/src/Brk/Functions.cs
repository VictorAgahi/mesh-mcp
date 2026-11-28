using Microsoft.Azure.Functions.Worker;

namespace Brk;

public class Functions
{
    [Function("Incoming")]
    [ServiceBusOutput("audit.log", Connection = "ServiceBusConnection")]
    public string Incoming([ServiceBusTrigger("%OrdersQueue%", Connection = "ServiceBusConnection")] string message) => message;

    [Function("Topic")]
    public void Topic([ServiceBusTrigger("price-updates", "price-sub", Connection = "ServiceBusConnection")] string message)
    {
    }
}
