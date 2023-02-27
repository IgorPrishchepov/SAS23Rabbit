using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities;
using System.Collections.Concurrent;
using Xunit.Sdk;

namespace RabbitMqTest.RPC
{
    public class RequestReplyHandler
    {
        public ConcurrentBag<string>? Messages { get; private set; }

        public RequestReplyHandler()
        {
            Messages = new ConcurrentBag<string>();
        }

        public async Task InvokeAsync(string quename, string message)
        {
            using var rpcClient = new RpcClient(quename);           
            var response = await rpcClient.CallAsync(message);
            Messages?.Add(response ?? "Error");      
        }
    }
}
