using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp4
{
    public class MessageProcessor : IMessageProcessor
    {
        ulong lastsequence = 0;
        public event Action<string>? MessageReceived;

        public void StartReadingMessages()
        {
            // Here we would start connection, create exchange, declare queue and binding, etc
        }

        private void OnMessageReceived(string message) //protected virtual method
        {
            MessageReceived?.Invoke(message);
        }

        public void StartReadingMessages(IModel channel, string queue)
        {
            Console.WriteLine("Start redaing");
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

    }
}
