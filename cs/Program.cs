
using ConsoleApp4;
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

try
{
    connection = factory.CreateConnection();
    channel = connection.CreateModel();

    channel.BasicQos(0, 2, false);

    connection.CallbackException += callBackException;
    connection.ConnectionBlocked += connectionBlocked;
    connection.ConnectionUnblocked += connectionUnblocked;
    connection.ConnectionUnblocked += connectionShutdown;

    channel.ExchangeDeclare(exchange: "DomExchange", type: ExchangeType.Topic, durable: true);

    var queueName = "quorum-test";

    string routingKey = "quorum";

    var arguments = new Dictionary<string, object> { { "x-queue-type", "quorum" } };

    channel.ModelShutdown += OnShutdown;

    var nodes = new List<string>();
    var erps = new List<string>();

    int numNodes = 1;
    int numErps = 1;

    for (int i = 0; i < numNodes; i++)
    {
        nodes.Add($"Node{i}");
    }

    for (int i = 0; i < numErps; i++)
    {
        erps.Add($"Erp{i}");
    }

    queueName = DeclareQueues(channel, arguments, nodes, erps);

    Console.WriteLine($"[*] Waiting for All in route {routingKey}.");

    StartMessageProcessor(channel, queueName);
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
            var myChannel = connection.CreateModel();
            myChannel.BasicQos(0, 2, false);
            myChannel.ModelShutdown += OnShutdown;
            DeclareQueues(myChannel, arguments, nodes, erps);
            StartMessageProcessor(myChannel, queueName);
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

void connectionShutdown(object? sender, EventArgs e)
{
    Console.WriteLine("Connection shutdown");
}

void connectionUnblocked(object? sender, EventArgs e)
{
    Console.WriteLine("Connection unblocked");
}

void connectionBlocked(object? sender, ConnectionBlockedEventArgs e)
{
    Console.WriteLine($"Connection blocked. Is open? {connection.IsOpen}");
}

void callBackException(object? sender, CallbackExceptionEventArgs e)
{
    Console.WriteLine($"Callback exception {e.Exception.Message}");
}

void OnShutdown(object? sender, ShutdownEventArgs e)
{
    Console.WriteLine($"Channel shutdown {e.ReplyCode} - {e.ReplyText}");
    if (e.ReplyCode == RabbitMQ.Client.Constants.ReplySuccess)
        return;
}

static string DeclareQueues(IModel channel , Dictionary<string, object> arguments, List<string> nodes, List<string> erps)
{
    string queueName = string.Empty;
    foreach (var erp in erps)
    {
        // DOM -> ERP
        queueName = $"DOM-{erp}";
        channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments);
        channel.QueueBind(queue: queueName, exchange: "DomExchange", routingKey: $"DOM.{erp}.*");

        // ERP -> DOM
        queueName = $"{erp}-DOM";
        channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments);
        channel.QueueBind(queue: queueName, exchange: "DomExchange", routingKey: $"{erp}.*");
    }

    foreach (var node in nodes)
    {
        // DOM -> Node
        queueName = $"DOM-{node}";
        channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments);
        channel.QueueBind(queue: queueName, exchange: "DomExchange", routingKey: $"DOM.{node}.*");

        // Node -> DOM
        queueName = $"{node}-DOM";
        channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments);
        channel.QueueBind(queue: queueName, exchange: "DomExchange", routingKey: $"{node}.DOM");

        foreach (var erp in erps)
        {
            // ERP -> Node
            queueName = $"{erp}-{node}";
            channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments);
            channel.QueueBind(queue: queueName, exchange: "DomExchange", routingKey: $"{erp}.ALL");

            // Node -> ERP
            queueName = $"{node}-{erp}";
            channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments);
            channel.QueueBind(queue: queueName, exchange: "DomExchange", routingKey: $"{node}.{erp}");
        }
    }

    return queueName;
}

static void StartMessageProcessor(IModel channel, string queueName)
{
    var messageProcessor = new MessageProcessor();
    messageProcessor.MessageReceived += (message) =>
    {
        Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}][{DateTime.Now:HH:mm:ss.fff}] Sleeping {message}");
        Thread.Sleep(30000);
        Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}][{DateTime.Now:HH:mm:ss.fff}] Slept {message}");
    };

    messageProcessor.StartReadingMessages(channel, queueName);
}
