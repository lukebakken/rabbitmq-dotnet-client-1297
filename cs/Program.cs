using System.Diagnostics;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory()
{
    HostName = "localhost",
    VirtualHost = "/",
    UserName = "guest",
    Password = "guest",
    ClientProvidedName = "TestClient",
    RequestedHeartbeat = TimeSpan.FromSeconds(10),
    AutomaticRecoveryEnabled = false,
    TopologyRecoveryEnabled = false,
};

IConnection? connection = null;
IModel? channel = null;

const string exchangeName = "gh-1297";
const string queueName = "gh-1297-queue";

try
{
    connection = factory.CreateConnection();
    channel = CreateModel(connection);

    connection.CallbackException += OnConnectionCallbackException;
    connection.ConnectionBlocked += OnConnectionBlocked;
    connection.ConnectionUnblocked += OnConnectionUnblocked;
    connection.ConnectionUnblocked += OnConnectionShutdown;

    DeclareEntities(channel, queueName, exchangeName);

    Console.WriteLine($"[*] waiting for messages via routing key: {queueName}.");
    var messageProcessor = new MessageProcessor();
    messageProcessor.MessageReceived += (message) =>
    {
        Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}][{DateTime.Now:HH:mm:ss.fff}] Sleeping {message}");
        Thread.Sleep(30000);
        Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}][{DateTime.Now:HH:mm:ss.fff}] Slept {message}");
    };

    messageProcessor.StartReadingMessages(channel, queueName);

    bool stop = false;

    Console.WriteLine(" Press [enter] to exit.");
    Console.CancelKeyPress += (sender, e) =>
    {
        stop = true;
    };

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
}
finally
{
    channel?.Close();
    channel?.Dispose();
    connection?.Close();
    connection?.Dispose();
}

static void DeclareEntities(IModel channel, string exchangeName, string queueName)
{
    channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Direct, durable: true);
    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false);
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
    Console.WriteLine("Connection shutdown");
}

void OnConnectionUnblocked(object? sender, EventArgs e)
{
    Console.WriteLine("Connection unblocked");
}

void OnConnectionBlocked(object? sender, ConnectionBlockedEventArgs e)
{
    Console.WriteLine($"Connection blocked. Is open? {connection.IsOpen}");
}

void OnConnectionCallbackException(object? sender, CallbackExceptionEventArgs e)
{
    Console.WriteLine($"Connection callback exception: {e.Exception.Message}");
}

void OnModelShutdown(object? sender, ShutdownEventArgs e)
{
    Console.WriteLine($"Channel shutdown: {e.ReplyCode} - {e.ReplyText}");
}

void OnModelCallbackException(object? sender, CallbackExceptionEventArgs e)
{
    Console.WriteLine($"Channel callback exception: {e.Exception.Message}");
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

void StartConsuming(IModel channel, string queueName)
{
    Console.WriteLine("[INFO} consumer is starting");
    var consumer = new EventingBasicConsumer(channel);
    Stopwatch sw = new Stopwatch();
    consumer.Received += (model, ea) =>
    {
        if (ea.DeliveryTag <= lastsequence)
            Console.WriteLine("OUT OF ORDER");
        else
        {
            if (model is not EventingBasicConsumer consumer)
                return;
            var myChannel = consumer.Model;
            lastsequence = ea.DeliveryTag;
            Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}][{DateTime.Now:HH:mm:ss.fff}] Received message {lastsequence} redelivered {ea.Redelivered}");
            sw.Restart();
            var message = Encoding.UTF8.GetString(ea.Body.Span);
            OnMessageReceived(message);
            //Console.WriteLine($"Message {message} readed. Press key to nack");
            //Console.ReadKey();
            //channel.BasicNack(ea.DeliveryTag, false, true);
            try
            {
                if (myChannel.IsOpen)
                {
                    myChannel.BasicAck(ea.DeliveryTag, false);
                    Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}][{DateTime.Now:HH:mm:ss.fff}] Confirmed message {lastsequence}");
                }
            }
            catch(Exception ex) { Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}][{DateTime.Now:HH:mm:ss.fff}] Error confirming message {lastsequence}"); }
        }

    };

    channel.BasicConsume(queue: queue,
                            autoAck: false,
                            consumer: consumer);

    consumer.Unregistered += UnregisteredAsync;
    consumer.ConsumerCancelled += Cancelled;
    consumer.Shutdown += Shutdown;
}

private void UnregisteredAsync(object? sender, ConsumerEventArgs @event)
{
    Console.WriteLine("Consumer unregistered");
}

private void Shutdown(object? sender, ShutdownEventArgs e)
{
    Console.WriteLine("Consumer shutdown");
}

private void Cancelled(object? sender, ConsumerEventArgs e)
{
    Console.WriteLine("Consumer cancelled");
}
