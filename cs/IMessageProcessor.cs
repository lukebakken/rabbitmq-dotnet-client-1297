
namespace ConsoleApp4
{
    public interface IMessageProcessor
    {
        event Action<string>? MessageReceived;

        void StartReadingMessages();
    }
}