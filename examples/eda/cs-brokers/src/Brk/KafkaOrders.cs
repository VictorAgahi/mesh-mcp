using Confluent.Kafka;

namespace Brk;

public class KafkaOrders
{
    private readonly IProducer<string, string> _producer;

    public KafkaOrders(IProducer<string, string> producer) => _producer = producer;

    public Task Send() => _producer.ProduceAsync("orders", new Message<string, string> { Value = "x" });
}
