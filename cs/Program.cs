using System.Diagnostics;
using System.Timers;
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

bool stopping = false;
var r = new Random();
var timers = new Dictionary<ulong, System.Timers.Timer>();
var stopEvent = new ManualResetEventSlim();
var channelShutdownEvent = new ManualResetEventSlim();

var handles = new WaitHandle[2];
handles[0] = stopEvent.WaitHandle;
handles[1] = channelShutdownEvent.WaitHandle;

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

    while (!stopping)
    {
        int i = WaitHandle.WaitAny(handles);
        switch (i)
        {
            case 0:
                // stopEvent was set
                stopping = true;
                break;
            case 1:
                // channelShutdown was set
                Console.WriteLine("[INFO] re-starting channel and consumer...");
                channel = CreateModel(connection);
                DeclareEntities(channel);
                consumerTag = StartConsuming(channel);
                break;
            default:
                continue;
        }
    }
}
finally
{
    Console.WriteLine("[INFO] disposing resources on exit...");
    StopTimers();
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
    channelShutdownEvent.Reset();
    var ch = connection.CreateModel();
    ch.BasicQos(0, 5, false);
    ch.ModelShutdown += OnModelShutdown;
    ch.CallbackException += OnModelCallbackException;
    ch.BasicAcks += OnModelBasicAcks;
    ch.BasicNacks += OnModelBasicNacks;
    ch.BasicReturn += OnModelBasicReturn;
    ch.FlowControl += OnModelFlowControl;
    return ch;
}

void StopTimers()
{
    if (timers is null)
    {
        Console.Error.WriteLine("[WARNING] timers dict is null, huh?");
    }
    else
    {
        foreach (KeyValuePair<ulong, System.Timers.Timer> e in timers)
        {
            var t = e.Value;
            t.Enabled = false;
            t.Dispose();
        }
        timers.Clear();
    }
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
    StopTimers();
    channelShutdownEvent.Set();
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
    consumer.Received += (objConsumer, ea) =>
    {
        if (stopEvent.IsSet)
        {
            Console.Error.WriteLine("[WARNING] consumer received message while cancellation requested!");
            channel.BasicCancel(consumer.ConsumerTags[0]);
            return;
        }

        if (ea.DeliveryTag <= lastDeliveryTag)
        {
            Console.Error.WriteLine("[ERROR] OUT OF ORDER");
        }
        else
        {
            Debug.Assert(Object.ReferenceEquals(objConsumer, consumer));
            Debug.Assert(Object.ReferenceEquals(channel, consumer.Model));

            lastDeliveryTag = ea.DeliveryTag;
            Console.WriteLine($"[INFO] [{Thread.CurrentThread.ManagedThreadId}][{DateTime.Now:HH:mm:ss.fff}] consumed message, deliveryTag: {lastDeliveryTag}, redelivered: {ea.Redelivered}");

            var timer = new System.Timers.Timer(r.Next(1000, 10000));
            timer.Elapsed += (object? objTimer, ElapsedEventArgs e) =>
            {
                ulong dt = ea.DeliveryTag;
                var t = (System.Timers.Timer?)objTimer;
                if (t is not null)
                {
                    try
                    {
                        t.Enabled = false;
                        try
                        {
                            var c = (EventingBasicConsumer)objConsumer;
                            c.Model.BasicAck(dt, false);
                            Console.WriteLine($"[INFO] [{Thread.CurrentThread.ManagedThreadId}][{DateTime.Now:HH:mm:ss.fff}] acked message {dt}");
                        }
                        catch (Exception ex)
                        {
                            Console.Error.WriteLine($"[ERROR] [{Thread.CurrentThread.ManagedThreadId}][{DateTime.Now:HH:mm:ss.fff}] Error confirming message {dt}, ex: {ex.Message}");
                        }
                    }
                    finally
                    {
                        timers.Remove(dt);
                        t.Dispose();
                    }
                }
            };
            timer.Enabled = true;
            timer.AutoReset = false;
            timers.Add(ea.DeliveryTag, timer);
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
