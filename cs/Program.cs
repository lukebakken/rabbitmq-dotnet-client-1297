using System.Diagnostics;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory()
{
    HostName = "localhost",
    VirtualHost = "/",
    UserName = "guest",
    Password = "guest",
    ClientProvidedName = "gh-1297-TestClient",
    RequestedHeartbeat = TimeSpan.FromSeconds(10),
    AutomaticRecoveryEnabled = false,
    TopologyRecoveryEnabled = false,
};

IConnection? connection = null;
IModel? channel = null;

const string exchangeName = "gh-1297";
const string queueName = "gh-1297-queue";

var stopEvent = new ManualResetEventSlim();

try
{
    connection = factory.CreateConnection();
    channel = CreateModel(connection);

    connection.CallbackException += OnConnectionCallbackException;
    connection.ConnectionBlocked += OnConnectionBlocked;
    connection.ConnectionUnblocked += OnConnectionUnblocked;
    connection.ConnectionUnblocked += OnConnectionShutdown;

    DeclareEntities(channel);

    var consumerTag = StartConsuming(channel);

    Console.CancelKeyPress += (object? sender, ConsoleCancelEventArgs e) =>
    {
        Console.WriteLine("[INFO] CTRL-C pressed, exiting!");
        stopEvent.Set();
        e.Cancel = true;
    };

    stopEvent.Wait();

    Console.WriteLine("[INFO] cancelling consumer on exit...");
    channel.BasicCancel(consumerTag);
    /*
    while (!stop)
    {
        if (connection.IsOpen && !channel.IsOpen)
        {
            channel.Close();
            channel.Dispose();
            channel = CreateModel(connection);
            DeclareEntities(channel, queueName, exchangeName);
            // TODO re-start consumer
        }
        else
            Thread.Sleep(2000);
    }
    */
}
finally
{
    Console.WriteLine("[INFO] disposing resources on exit...");
    channel?.Close();
    channel?.Dispose();
    connection?.Close();
    connection?.Dispose();
}

void DeclareEntities(IModel channel)
{
    channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Direct, durable: true);
    channel.QueueDeclare(queue: queueName, durable: true, exclusive: false, autoDelete: false);
    channel.QueueBind(queue: queueName, exchange: exchangeName, routingKey: queueName);
}

IModel CreateModel(IConnection connection)
{
    var ch = connection.CreateModel();
    ch.BasicQos(0, 2, false);
    ch.ModelShutdown += OnModelShutdown;
    ch.CallbackException += OnModelCallbackException;
    ch.BasicAcks += OnModelBasicAcks;
    ch.BasicNacks += OnModelBasicNacks;
    ch.BasicReturn += OnModelBasicReturn;
    ch.FlowControl += OnModelFlowControl;
    return ch;
}

void OnConnectionShutdown(object? sender, EventArgs e)
{
    Console.WriteLine("[INFO] connection shutdown");
}

void OnConnectionUnblocked(object? sender, EventArgs e)
{
    Console.WriteLine("[INFO] connection unblocked");
}

void OnConnectionBlocked(object? sender, ConnectionBlockedEventArgs e)
{
    Console.WriteLine($"[INFO] connection blocked, IsOpen: {connection.IsOpen}");
}

void OnConnectionCallbackException(object? sender, CallbackExceptionEventArgs e)
{
    Console.Error.WriteLine($"[ERROR] connection callback exception: {e.Exception.Message}");
}

void OnModelShutdown(object? sender, ShutdownEventArgs e)
{
    Console.WriteLine($"[INFO] channel shutdown: {e.ReplyCode} - {e.ReplyText}");
}

void OnModelCallbackException(object? sender, CallbackExceptionEventArgs e)
{
    Console.Error.WriteLine($"[ERROR] channel callback exception: {e.Exception.Message}");
}

void OnModelBasicAcks(object? sender, BasicAckEventArgs e)
{
    Console.WriteLine($"Channel saw basic.ack, delivery tag: {e.DeliveryTag} multiple: {e.Multiple}");
}

void OnModelBasicNacks(object? sender, BasicNackEventArgs e)
{
    Console.WriteLine($"Channel saw basic.nack, delivery tag: {e.DeliveryTag} multiple: {e.Multiple}");
}

void OnModelBasicReturn(object? sender, BasicReturnEventArgs e)
{
    Console.WriteLine($"Channel saw basic.return {e.ReplyCode} - {e.ReplyText}");
}

void OnModelFlowControl(object? sender, FlowControlEventArgs e)
{
    Console.WriteLine($"Channel saw flow control, active: {e.Active}");
}

string StartConsuming(IModel channel)
{
    ulong lastDeliveryTag = 0;

    Console.WriteLine($"[INFO] consumer is starting, queueName: {queueName}");
    var consumer = new EventingBasicConsumer(channel);
    consumer.Received += (sender, ea) =>
    {
        if (stopEvent.IsSet)
        {
            Console.WriteLine("[WARNING] consumer received message while cancellation requested!");
        }

        if (ea.DeliveryTag <= lastDeliveryTag)
        {
            Console.Error.WriteLine("[ERROR] OUT OF ORDER");
        }
        else
        {
            Debug.Assert(Object.ReferenceEquals(sender, consumer));
            Debug.Assert(Object.ReferenceEquals(channel, consumer.Model));

            lastDeliveryTag = ea.DeliveryTag;
            Console.WriteLine($"[INFO] [{Thread.CurrentThread.ManagedThreadId}][{DateTime.Now:HH:mm:ss.fff}] consumed message, deliveryTag: {lastDeliveryTag}, redelivered: {ea.Redelivered}");
            try
            {
                var ch = consumer.Model;
                ch.BasicAck(ea.DeliveryTag, false);
                Console.WriteLine($"[INFO] [{Thread.CurrentThread.ManagedThreadId}][{DateTime.Now:HH:mm:ss.fff}] acked message {lastDeliveryTag}");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ERROR] [{Thread.CurrentThread.ManagedThreadId}][{DateTime.Now:HH:mm:ss.fff}] Error confirming message {lastDeliveryTag}, ex: {ex.Message}");
            }
        }
    };

    consumer.Unregistered += OnConsumerUnregistered;
    consumer.ConsumerCancelled += OnConsumerCancelled;
    consumer.Shutdown += OnConsumerShutdown;

    var consumerTag = channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);
    Console.WriteLine($"[INFO] consumer started, consumerTag: {consumerTag}");
    return consumerTag;
}

void OnConsumerUnregistered(object? sender, ConsumerEventArgs e)
{
    Console.WriteLine($"[INFO] consumer unregistered, consumer tag: {e.ConsumerTags[0]}");
}

void OnConsumerCancelled(object? sender, ConsumerEventArgs e)
{
    Console.WriteLine($"[INFO] consumer cancelled, consumer tag: {e.ConsumerTags[0]}");
}

void OnConsumerShutdown(object? sender, ShutdownEventArgs e)
{
    Console.WriteLine($"[INFO] consumer shutdown {e.ReplyCode} - {e.ReplyText}");
}
