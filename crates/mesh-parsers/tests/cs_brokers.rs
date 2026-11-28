//! C# brokers (9.0 T8d) through the real dispatch (`PolyglotIndexer`) and the graph: MassTransit,
//! NServiceBus and Azure Service Bus. The golden truth of the fixture is
//! `tests/golden/events/cs-brokers.expected.yaml`; these tests pin the idioms the fixture cannot
//! show on its own line (gates, shadowing, constants, defaults, secrets, caps).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mesh_core::{Broker, ContractGraph};
use mesh_parsers::languages::{PolyglotIndexer, MAX_CS_BROKER_SITES_PER_FILE};
use mesh_parsers::FileIndex;
use std::path::Path;

type Site = (char, String, String);

fn extract(code: &str) -> FileIndex {
    let fi = PolyglotIndexer::extract(Path::new("src/Svc/App.cs"), code, 0);
    assert!(!fi.parse_failed, "must parse");
    fi
}

/// `(role, broker slug, topic)` of every producer and consumer, sorted. A site without a broker
/// reads `?`.
fn sites(fi: &FileIndex) -> Vec<Site> {
    let slug = |brokers: &[(usize, Broker)], i: usize| {
        brokers
            .iter()
            .find(|(j, _)| *j == i)
            .map_or("?", |(_, b)| b.slug())
            .to_string()
    };
    let mut out: Vec<_> = fi
        .producers
        .iter()
        .enumerate()
        .map(|(i, (_, t))| ('P', slug(&fi.producer_brokers, i), t.to_string()))
        .chain(
            fi.consumers
                .iter()
                .enumerate()
                .map(|(i, (_, t))| ('C', slug(&fi.consumer_brokers, i), t.to_string())),
        )
        .collect();
    out.sort();
    out
}

fn s(role: char, broker: &str, topic: &str) -> Site {
    (role, broker.to_string(), topic.to_string())
}

fn read(code: &str) -> Vec<Site> {
    sites(&extract(code))
}

// ---- MassTransit -------------------------------------------------------------------------

#[test]
fn a_masstransit_consumer_is_keyed_by_its_message_type_with_no_broker() {
    let code = r##"
using MassTransit;
namespace Svc;
public class OrderCreatedConsumer : IConsumer<OrderCreated>, IConsumer<Batch<OrderPaid>>
{
    public Task Consume(ConsumeContext<OrderCreated> context) => Task.CompletedTask;
}
"##;
    assert_eq!(
        read(code),
        vec![s('C', "?", "OrderCreated"), s('C', "?", "OrderPaid")]
    );
}

#[test]
fn masstransit_types_need_the_using_and_a_single_type_argument() {
    // No using: not MassTransit's IConsumer.
    assert!(read("public class A : IConsumer<OrderCreated> { }").is_empty());
    // Confluent.Kafka's IConsumer<TKey, TValue>, with MassTransit imported too.
    let kafka = "using MassTransit;\nusing Confluent.Kafka;\npublic class A : IConsumer<string, string> { }";
    assert!(read(kafka).is_empty());
    // A fully qualified base type needs no using.
    let qualified = "public class A : MassTransit.IConsumer<OrderCreated> { }";
    assert_eq!(read(qualified), vec![s('C', "?", "OrderCreated")]);
    // A look-alike namespace is not MassTransit.
    let lookalike = "using MassTransitShim;\npublic class A : IConsumer<OrderCreated> { }";
    assert!(read(lookalike).is_empty());
}

#[test]
fn a_local_type_named_like_a_client_hides_it() {
    let code = r##"
using MassTransit;
public interface IConsumer<T> { }
public class IPublishEndpoint { public void Publish(object o) { } }
public class A : IConsumer<OrderCreated> { }
public class B
{
    private readonly IPublishEndpoint _bus;
    public void Run() { _bus.Publish(new OrderCreated()); }
}
"##;
    assert!(read(code).is_empty());
    // The same file without the local declarations reads both sites.
    let real = r##"
using MassTransit;
public class A : IConsumer<OrderCreated> { }
public class B
{
    private readonly IPublishEndpoint _bus;
    public void Run() { _bus.Publish(new OrderCreated()); }
}
"##;
    assert_eq!(
        read(real),
        vec![s('C', "?", "OrderCreated"), s('P', "?", "OrderCreated")]
    );
}

#[test]
fn a_using_alias_of_a_client_name_hides_it() {
    let code = r##"
using MassTransit;
using IPublishEndpoint = Homegrown.Bus;
public class B
{
    private readonly IPublishEndpoint _bus;
    public void Run() { _bus.Publish(new OrderCreated()); }
}
"##;
    assert!(read(code).is_empty());
}

#[test]
fn publish_and_send_read_the_message_type_the_file_shows() {
    let code = r##"
using MassTransit;
public class Svc
{
    private readonly IPublishEndpoint _publish;
    private readonly ISendEndpointProvider _provider;
    public IBus Bus { get; }
    public Svc(IPublishEndpoint publish, ISendEndpointProvider provider) { _publish = publish; _provider = provider; }

    public async Task A(OrderPaid paid, object any, string text)
    {
        await _publish.Publish<OrderCreated>(new { Id = 1 });
        await _publish.Publish(new OrderShipped { Id = 2 });
        await this._publish.Publish(paid);
        await _publish.Publish(any);
        await _publish.Publish(text);
        await Bus.Send(new OrderCancelled());
        var made = new OrderRefunded();
        await _publish?.Publish(made);
    }
    public async Task B(ConsumeContext<OrderCreated> context, IBusControl control)
    {
        await context.Publish(new OrderAck());
        await control.Publish<OrderDone>(new { });
    }
}
"##;
    assert_eq!(
        read(code),
        vec![
            s('P', "?", "OrderAck"),
            s('P', "?", "OrderCancelled"),
            s('P', "?", "OrderCreated"),
            s('P', "?", "OrderDone"),
            s('P', "?", "OrderPaid"),
            s('P', "?", "OrderRefunded"),
            s('P', "?", "OrderShipped"),
        ]
    );
}

#[test]
fn a_homegrown_event_bus_and_unrelated_publish_are_no_producers() {
    let code = r##"
using MassTransit;
public class Svc
{
    private readonly IEventBus _events;
    private readonly Notifier _notifier;
    public void A(IPublishEndpoint bus)
    {
        _events.Publish(new OrderCreated());
        _notifier.Publish(new OrderCreated());
        Publish(new OrderCreated());
        Func<Task> g = async () => await _events.Send(new OrderCreated());
    }
    public void B(Action<IBus> cfg) { cfg.Invoke(null); }
}
"##;
    assert!(read(code).is_empty());
}

#[test]
fn a_lambda_or_local_parameter_hides_a_typed_field() {
    let code = r##"
using MassTransit;
public class Svc
{
    private readonly IPublishEndpoint bus;
    public void A()
    {
        Run(bus => bus.Publish(new Hidden()));
        Run2((IEventBus bus) => bus.Publish(new Hidden2()));
    }
    public void B(IEventBus bus) { bus.Publish(new Hidden3()); }
    public void C() { bus.Publish(new Visible()); }
}
"##;
    assert_eq!(read(code), vec![s('P', "?", "Visible")]);
}

#[test]
fn a_generic_type_parameter_is_no_message_type() {
    let code = r##"
using MassTransit;
public class Svc
{
    private readonly IPublishEndpoint _bus;
    public Task Emit<TEvent>(TEvent evt) => _bus.Publish<TEvent>(evt);
    public Task Real() => _bus.Publish<OrderCreated>(new { });
}
"##;
    assert_eq!(read(code), vec![s('P', "?", "OrderCreated")]);
}

#[test]
fn the_bus_is_read_from_the_using_call_of_the_same_file() {
    let rabbit = r##"
using MassTransit;
public class Setup
{
    public void Configure(IServiceCollection services)
    {
        services.AddMassTransit(x => x.UsingRabbitMq((ctx, cfg) => cfg.Host("localhost")));
    }
    private readonly IPublishEndpoint _bus;
    public Task Go() => _bus.Publish(new OrderCreated());
}
public class OrderCreatedConsumer : IConsumer<OrderCreated> { }
"##;
    assert_eq!(
        read(rabbit),
        vec![
            s('C', "rabbitmq", "OrderCreated"),
            s('P', "rabbitmq", "OrderCreated")
        ]
    );
    let asb = rabbit.replace("UsingRabbitMq", "UsingAzureServiceBus");
    assert_eq!(read(&asb)[0], s('C', "servicebus", "OrderCreated"));
    let sqs = rabbit.replace("UsingRabbitMq", "UsingAmazonSqs");
    assert_eq!(read(&sqs)[0], s('C', "sqs", "OrderCreated"));
    let mem = rabbit.replace("UsingRabbitMq", "UsingInMemory");
    assert_eq!(read(&mem)[0], s('C', "inproc", "OrderCreated"));
    // Two different buses in one file: which one a message type uses is not known.
    let both = rabbit.replace(
        "services.AddMassTransit",
        "services.AddMassTransit(x => x.UsingAzureServiceBus((c, f) => { }));\n        services.AddMassTransit",
    );
    assert_eq!(read(&both)[0], s('C', "?", "OrderCreated"));
    // A transport with no broker key: no broker either.
    let amq = rabbit.replace("UsingRabbitMq", "UsingActiveMq");
    assert_eq!(read(&amq)[0], s('C', "?", "OrderCreated"));
}

#[test]
fn a_receive_endpoint_is_a_consumer_of_its_queue_inside_a_using_call_only() {
    let code = r##"
using MassTransit;
public class Setup
{
    public void A(IServiceCollection services)
    {
        services.AddMassTransit(x =>
        {
            x.UsingRabbitMq((ctx, cfg) =>
            {
                cfg.ReceiveEndpoint("order-queue", e => { e.ConfigureConsumer<OrderConsumer>(ctx); });
                cfg.ReceiveEndpoint(e => { });
                cfg.ReceiveEndpoint(Dynamic.Name, e => { });
            });
        });
    }
    public void B(Foo other) { other.ReceiveEndpoint("not-masstransit", e => { }); }
}
"##;
    assert_eq!(read(code), vec![s('C', "rabbitmq", "order-queue")]);
    let bus = code.replace("UsingRabbitMq", "UsingAzureServiceBus");
    assert_eq!(read(&bus), vec![s('C', "servicebus", "order-queue")]);
    // Without the MassTransit using, the call is nobody's.
    assert!(read(&code.replace("using MassTransit;", "")).is_empty());
}

#[test]
fn get_send_endpoint_names_the_queue_of_a_short_address_only() {
    let code = r##"
using MassTransit;
public class Svc
{
    private readonly ISendEndpointProvider _provider;
    public async Task Go(ISendEndpointProvider other, IBus bus)
    {
        await _provider.GetSendEndpoint(new Uri("queue:orders?durable=false"));
        await other.GetSendEndpoint(new Uri("exchange:order-events"));
        await bus.GetSendEndpoint(new Uri("rabbitmq://host/vhost/queue"));
        await bus.GetSendEndpoint(new Uri(Settings.Address));
        await _provider.GetSendEndpoint(new Uri("queue:" + name));
    }
}
"##;
    assert_eq!(
        read(code),
        vec![s('P', "?", "order-events"), s('P', "?", "orders")]
    );
}

// ---- NServiceBus -------------------------------------------------------------------------

#[test]
fn nservicebus_handlers_and_sends_are_keyed_by_the_message_type() {
    let code = r##"
using NServiceBus;
public class PlaceOrderHandler : IHandleMessages<PlaceOrder>
{
    public async Task Handle(PlaceOrder message, IMessageHandlerContext context)
    {
        await context.Publish(new OrderPlaced());
        await context.Send("billing", new BillOrder());
        await context.SendLocal(new AuditOrder());
        await context.Reply(new OrderAccepted());
        await context.Publish<OrderLogged>();
    }
}
public class OrderSaga : Saga<Data>, IAmStartedByMessages<StartOrder>, IHandleMessages<ShipOrder> { }
public class Starter
{
    private readonly IMessageSession _session;
    public Task Go(IEndpointInstance endpoint) => endpoint.Send(new StartOrder());
}
"##;
    assert_eq!(
        read(code),
        vec![
            s('C', "?", "PlaceOrder"),
            s('C', "?", "ShipOrder"),
            s('C', "?", "StartOrder"),
            s('P', "?", "AuditOrder"),
            s('P', "?", "BillOrder"),
            s('P', "?", "OrderAccepted"),
            s('P', "?", "OrderLogged"),
            s('P', "?", "OrderPlaced"),
            s('P', "?", "StartOrder"),
        ]
    );
}

#[test]
fn nservicebus_needs_its_using_and_ignores_other_handler_frameworks() {
    // Rebus has an IHandleMessages<T> too.
    let rebus = "using Rebus.Handlers;\npublic class H : IHandleMessages<PlaceOrder> { }";
    assert!(read(rebus).is_empty());
    let local = "using NServiceBus;\npublic interface IHandleMessages<T> { }\npublic class H : IHandleMessages<PlaceOrder> { }";
    assert!(read(local).is_empty());
    // MassTransit's and NServiceBus's names never cross.
    let mixed = "using MassTransit;\npublic class H : IHandleMessages<PlaceOrder> { }";
    assert!(read(mixed).is_empty());
    let mixed2 = "using NServiceBus;\npublic class H : IConsumer<PlaceOrder> { }";
    assert!(read(mixed2).is_empty());
}

#[test]
fn the_nservicebus_transport_names_the_broker_of_the_file() {
    let rabbit = r##"
using NServiceBus;
public class Setup
{
    public EndpointConfiguration Make()
    {
        var cfg = new EndpointConfiguration("Sales");
        cfg.UseTransport<RabbitMQTransport>();
        return cfg;
    }
}
public class H : IHandleMessages<PlaceOrder> { }
"##;
    assert_eq!(read(rabbit), vec![s('C', "rabbitmq", "PlaceOrder")]);
    let asb = rabbit.replace("RabbitMQTransport", "AzureServiceBusTransport");
    assert_eq!(read(&asb), vec![s('C', "servicebus", "PlaceOrder")]);
    let v8 = rabbit.replace(
        "cfg.UseTransport<RabbitMQTransport>();",
        "cfg.UseTransport(new SqsTransport());",
    );
    assert_eq!(read(&v8), vec![s('C', "sqs", "PlaceOrder")]);
    let learning = rabbit.replace("RabbitMQTransport", "LearningTransport");
    assert_eq!(read(&learning), vec![s('C', "?", "PlaceOrder")]);
}

// ---- Azure Service Bus -------------------------------------------------------------------

#[test]
fn service_bus_senders_processors_and_receivers_key_on_the_queue_or_topic() {
    let code = r##"
using Azure.Messaging.ServiceBus;
public class Svc
{
    private const string OrdersQueue = "orders";
    private static readonly string Topic = "order-events";
    private readonly ServiceBusClient _client;
    public async Task Go(ServiceBusClient other)
    {
        var a = _client.CreateSender(OrdersQueue);
        var b = other.CreateSender(Topic);
        var c = _client.CreateProcessor("invoices", new ServiceBusProcessorOptions());
        var d = _client.CreateProcessor("billing", "billing-sub");
        var e = _client.CreateReceiver("refunds");
        var f = _client.CreateSessionProcessor("sessions");
        var g = new ServiceBusClient("conn").CreateSender("direct");
        await using var local = new ServiceBusClient("conn");
        var h = local.CreateSender("via-var");
    }
}
"##;
    assert_eq!(
        read(code),
        vec![
            s('C', "servicebus", "billing"),
            s('C', "servicebus", "invoices"),
            s('C', "servicebus", "refunds"),
            s('C', "servicebus", "sessions"),
            s('P', "servicebus", "direct"),
            s('P', "servicebus", "order-events"),
            s('P', "servicebus", "orders"),
            s('P', "servicebus", "via-var"),
        ]
    );
}

#[test]
fn service_bus_calls_need_the_using_and_a_typed_client() {
    let code = r##"
public class Svc
{
    private readonly ServiceBusClient _client;
    public void Go() { _client.CreateSender("orders"); }
}
"##;
    assert!(read(code).is_empty());
    let shadow = format!(
        "using Azure.Messaging.ServiceBus;\npublic class ServiceBusClient {{ public object CreateSender(string q) => null; }}\n{}",
        code.replace("public class Svc", "public class Svc2")
    );
    assert!(read(&shadow).is_empty());
    let unrelated = "using Azure.Messaging.ServiceBus;\npublic class Svc { private readonly Cache _c; public void Go() { _c.CreateSender(\"orders\"); } }";
    assert!(read(unrelated).is_empty());
}

#[test]
fn service_bus_trigger_and_output_attributes_register_on_their_function() {
    let code = r##"
using Microsoft.Azure.Functions.Worker;
public class Functions
{
    [Function("A")]
    [ServiceBusOutput("out-queue", Connection = "ServiceBusConnection")]
    public string A([ServiceBusTrigger("in-queue", Connection = "ServiceBusConnection")] string message) => message;

    [Function("B")]
    public void B([ServiceBusTrigger("topic-a", "sub-a", Connection = "ServiceBusConnection")] string message) { }

    [Function("C")]
    public void C([ServiceBusTrigger("%OrdersQueue%")] string message) { }

    [Function("D")]
    public void D([ServiceBusTrigger(Constants.Q)] string message) { }

    [Function("E")]
    public void E([ServiceBusTrigger(Computed())] string message) { }
}
public static class Constants { public const string Q = "const-queue"; }
"##;
    assert_eq!(
        read(code),
        vec![
            s('C', "servicebus", "${OrdersQueue}"),
            s('C', "servicebus", "const-queue"),
            s('C', "servicebus", "in-queue"),
            s('C', "servicebus", "topic-a"),
            s('P', "servicebus", "out-queue"),
        ]
    );
    // The in-process WebJobs attributes.
    let webjobs = r##"
using Microsoft.Azure.WebJobs;
public class F
{
    [FunctionName("A")]
    [return: ServiceBus("wj-out")]
    public static string A([ServiceBusTrigger("wj-in")] string m) => m;
}
"##;
    assert_eq!(
        read(webjobs),
        vec![
            s('C', "servicebus", "wj-in"),
            s('P', "servicebus", "wj-out")
        ]
    );
}

#[test]
fn a_service_bus_attribute_needs_its_namespace_and_no_local_type_of_that_name() {
    let none = "public class F { public void A([ServiceBusTrigger(\"q\")] string m) { } }";
    assert!(read(none).is_empty());
    let local = "using Microsoft.Azure.WebJobs;\npublic class ServiceBusTrigger : System.Attribute { public ServiceBusTrigger(string q) { } }\npublic class F { public void A([ServiceBusTrigger(\"q\")] string m) { } }";
    assert!(read(local).is_empty());
    // A %Setting% outside a Functions attribute is no placeholder.
    let setting = "using Azure.Messaging.ServiceBus;\npublic class F { private readonly ServiceBusClient _c; public void A() { _c.CreateSender(\"%Q%\"); } }";
    assert!(read(setting).is_empty());
}

// ---- names, placeholders, defaults and secrets ---------------------------------------------

#[test]
fn configuration_reads_are_placeholders_and_a_default_is_never_a_name() {
    let code = r##"
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
public class Svc
{
    private readonly ServiceBusClient _client;
    private readonly IConfiguration _config;
    public void Go(IConfiguration configuration)
    {
        _client.CreateSender(configuration["ServiceBus:OrdersQueue"]);
        _client.CreateSender(_config["Orders:Topic"] ?? "fallback-topic");
        _client.CreateSender(_config.GetValue<string>("Billing:Queue", "billing-default"));
        _client.CreateSender(Environment.GetEnvironmentVariable("REFUND_QUEUE"));
        _client.CreateSender(configuration["Plain"]!);
        _client.CreateProcessor(configuration.GetSection("A")["B"]);
    }
}
"##;
    assert_eq!(
        read(code),
        vec![
            s('P', "servicebus", "${Billing__Queue}"),
            s('P', "servicebus", "${Orders__Topic}"),
            s('P', "servicebus", "${Plain}"),
            s('P', "servicebus", "${REFUND_QUEUE}"),
            s('P', "servicebus", "${ServiceBus__OrdersQueue}"),
        ]
    );
    let code_text = format!("{:?}", extract(code));
    assert!(!code_text.contains("fallback-topic"), "{code_text}");
    assert!(!code_text.contains("billing-default"), "{code_text}");
}

#[test]
fn a_configuration_read_needs_the_configuration_type() {
    let code = r##"
using Azure.Messaging.ServiceBus;
public class Svc
{
    private readonly ServiceBusClient _client;
    public void Go(Dictionary<string, string> settings, MyConfig custom)
    {
        _client.CreateSender(settings["Queue"]);
        _client.CreateSender(custom.GetValue<string>("Queue"));
        _client.CreateSender(Environment.GetEnvironmentVariable("Q"));
    }
}
"##;
    assert_eq!(read(code), vec![s('P', "servicebus", "${Q}")]);
    // A local `Environment` is not System.Environment.
    let local = code.replace(
        "public class Svc",
        "public class Environment { public static string GetEnvironmentVariable(string k) => k; }\npublic class Svc",
    );
    assert!(read(&local).is_empty());
}

#[test]
fn a_credential_keyed_setting_or_a_secret_shaped_literal_is_never_a_name() {
    let key = ["Shared", "Access", "Key"].concat();
    let canary = ["zq", "7Rk2", "mVp9", "Lw4t"].concat();
    let conn = format!("Endpoint=sb://ns.example.net/;{key}Name=root;{key}={canary}");
    let code = format!(
        r##"
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
public class Svc
{{
    private const string Conn = "{conn}";
    private const string ApiKey = "plain-name";
    private const string Token = "orders";
    private readonly ServiceBusClient _client;
    public void Go(IConfiguration configuration)
    {{
        _client.CreateSender("{conn}");
        _client.CreateSender(Conn);
        _client.CreateSender(ApiKey);
        _client.CreateSender(Token);
        _client.CreateSender(configuration["ServiceBus:ConnectionString"]);
        _client.CreateSender(configuration["Auth:ApiKey"]);
        _client.CreateSender(Environment.GetEnvironmentVariable("DB_PASSWORD"));
        _client.CreateSender("hunter2-prod-x");
        _client.CreateSender("postgres://u:pw@h/db");
        _client.CreateSender("a3f9c1e7b2d4");
        _client.CreateSender("orders.*");
        _client.CreateProcessor(configuration["Real:Queue"]);
        _client.CreateProcessor("password-reset-jobs");
    }}
}}
"##
    );
    let fi = extract(&code);
    assert_eq!(
        sites(&fi),
        vec![
            s('C', "servicebus", "${Real__Queue}"),
            s('C', "servicebus", "password-reset-jobs"),
        ]
    );
    let dumped = format!("{fi:?}");
    assert!(!dumped.contains(&canary), "{dumped}");
    assert!(
        !dumped.to_ascii_lowercase().contains("sharedaccesskey"),
        "{dumped}"
    );
    let json = serde_json::to_string(&fi).unwrap();
    assert!(!json.contains(&canary), "{json}");
}

#[test]
fn a_secret_on_an_attribute_line_does_not_reach_the_node_signature() {
    let canary = ["Zk", "3qP8", "vN2x", "Wm5"].concat();
    let code = format!(
        r##"
using Microsoft.Azure.Functions.Worker;
public class F
{{
    [ServiceBusTrigger("%Q%", Connection = "Endpoint=sb://h.example/;AccessKey={canary}")]
    public void A() {{ }}
    [ServiceBusTrigger("q-ok", Connection = "ServiceBusConnection")]
    public void B() {{ }}
    [Route("api/[controller]")]
    public void C() {{ }}
}}
"##
    );
    let fi = extract(&code);
    let dumped = format!("{fi:?}");
    assert!(!dumped.contains(&canary), "{dumped}");
    // A literal that is a plain setting name or a route stays readable.
    assert!(dumped.contains("ServiceBusConnection"), "{dumped}");
    assert!(dumped.contains("api/[controller]"), "{dumped}");
    assert!(dumped.contains("REDACTED_SECRET"), "{dumped}");
    assert_eq!(
        sites(&fi),
        vec![s('C', "servicebus", "${Q}"), s('C', "servicebus", "q-ok")]
    );
}

#[test]
fn constants_resolve_through_a_class_of_the_file_and_conflicts_read_as_nothing() {
    let code = r##"
using Azure.Messaging.ServiceBus;
public static class Queues
{
    public const string Orders = "orders";
    public const string Alias = Orders;
    public const string Dup = "one";
}
public static class Other { public const string Dup = "two"; }
public class Svc
{
    private readonly ServiceBusClient _client;
    public void Go(string Orders)
    {
        _client.CreateSender(Queues.Orders);
        _client.CreateSender(Queues.Alias);
        _client.CreateSender(Queues.Dup);
        _client.CreateSender(Remote.Queue);
        _client.CreateSender(Orders);
    }
}
"##;
    assert_eq!(
        read(code),
        vec![
            s('P', "servicebus", "orders"),
            s('P', "servicebus", "orders")
        ]
    );
}

// ---- Kafka stays as it was ------------------------------------------------------------------

#[test]
fn confluent_kafka_keeps_its_broker_and_a_masstransit_site_is_never_tagged_kafka() {
    let code = r##"
using Confluent.Kafka;
using MassTransit;
public class Svc
{
    private readonly IProducer<string, string> _producer;
    private readonly IPublishEndpoint _bus;
    public async Task Go()
    {
        await _producer.ProduceAsync("kafka-topic", new Message<string, string>());
        await _bus.Publish(new OrderCreated());
    }
}
"##;
    assert_eq!(
        read(code),
        vec![s('P', "?", "OrderCreated"), s('P', "kafka", "kafka-topic")]
    );
}

// ---- shapes without declarations, bounds -----------------------------------------------------

#[test]
fn top_level_statements_register_their_sites_on_a_call_node() {
    let code = r##"
using Azure.Messaging.ServiceBus;
var client = new ServiceBusClient("conn");
var sender = client.CreateSender("top-queue");
"##;
    let fi = extract(code);
    assert_eq!(sites(&fi), vec![s('P', "servicebus", "top-queue")]);
    assert!(fi
        .nodes
        .iter()
        .any(|n| n.name == "top-queue" && n.line_start == 4));
}

#[test]
fn a_file_past_the_site_cap_is_cut_and_flagged_never_silently() {
    let mut code = String::from(
        "using MassTransit;\npublic class Big\n{\n    private readonly IPublishEndpoint _bus;\n    public void Go()\n    {\n",
    );
    for i in 0..MAX_CS_BROKER_SITES_PER_FILE + 50 {
        code.push_str(&format!("        _bus.Publish(new Evt{i}());\n"));
    }
    code.push_str("    }\n}\n");
    let fi = extract(&code);
    assert!(fi.cs_brokers_capped);
    assert_eq!(fi.producers.len(), MAX_CS_BROKER_SITES_PER_FILE);
    assert!(sites(&fi).contains(&s('P', "?", "Evt0")));
    let small = extract("using MassTransit;\npublic class A { IPublishEndpoint _b; void G() { _b.Publish(new E()); } }");
    assert!(!small.cs_brokers_capped);
}

#[test]
fn deep_nesting_and_a_hostile_file_are_read_in_bounded_time() {
    let mut code = String::from(
        "using MassTransit;\npublic class A\n{\n    private readonly IPublishEndpoint _bus;\n    public void Go()\n    {\n",
    );
    let depth = 40;
    code.push_str(&"{ ".repeat(depth));
    code.push_str("_bus.Publish(new Deep());");
    code.push_str(&" }".repeat(depth));
    code.push_str("\n    }\n}\n");
    assert_eq!(read(&code), vec![s('P', "?", "Deep")]);
    // A file with a thousand unrelated classes and no site.
    let mut many = String::from("using MassTransit;\n");
    for i in 0..1000 {
        many.push_str(&format!(
            "public class C{i} : Base{i} {{ void M(int x) {{ x.ToString(); }} }}\n"
        ));
    }
    assert!(read(&many).is_empty());
}

// ---- the graph ---------------------------------------------------------------------------------

#[test]
fn one_name_on_two_brokers_is_two_channels_and_an_unknown_one_joins_both() {
    let mut graph = ContractGraph::new();
    for (file, code) in [
        (
            "svc/Setup.cs",
            "using MassTransit;\npublic class S { void A(IServiceCollection s) { s.AddMassTransit(x => x.UsingRabbitMq((c, f) => { })); }\n private readonly IPublishEndpoint _b; void G() { _b.Publish(new Orders()); } }\n",
        ),
        (
            "svc/Bus.cs",
            "using Azure.Messaging.ServiceBus;\npublic class B { private readonly ServiceBusClient _c; void G() { _c.CreateSender(\"Orders\"); } }\n",
        ),
        (
            "svc/Consumer.cs",
            "using MassTransit;\npublic class OrdersConsumer : IConsumer<Orders> { }\n",
        ),
    ] {
        PolyglotIndexer::index_file(Path::new(file), code, 0, &mut graph);
    }
    graph.reconcile_edges();
    let hubs: Vec<String> = graph
        .all_nodes()
        .filter(|n| n.package == "event-bus" && n.name.ends_with(":orders"))
        .map(|n| n.name.to_string())
        .collect();
    assert!(hubs.contains(&"rabbitmq:orders".to_string()), "{hubs:?}");
    assert!(hubs.contains(&"servicebus:orders".to_string()), "{hubs:?}");
}
