using FluentAssertions;
using RabbitMqTest.RPC;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Security;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMqTest
{
    public class RpcTests
    {
        const string QueueFour = "queue_4";
        const string TestMessageOne = "Hello, world";
        const string TestMessageTwo = "hello, World";

        [Fact(DisplayName = "Verify that RPC Server processes up to 4 requests in less than 1 minute")]
        public async Task RpcOneServerTest()
        {
            new RpcServer().DeclareQueue(QueueFour);

            var handler = new RequestReplyHandler();

            var tasks = new List<Task>();


            for (int i = 0; i < 2; i++)
            {
                tasks.Add(handler.InvokeAsync(QueueFour, TestMessageOne).WaitAsync(TimeSpan.FromSeconds(30)));
                tasks.Add(handler.InvokeAsync(QueueFour, TestMessageTwo).WaitAsync(TimeSpan.FromSeconds(30)));
            }

            DateTime start = DateTime.UtcNow;

            await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60));

            DateTime end = DateTime.UtcNow;
            TimeSpan timeDiff = (end - start);

            handler?.Messages?.Count.Should().Be(4, because: "Not all messages were processed");
            handler?.Messages?.All(m => m.Equals("HELLO, WORLD"));
        }

        [Fact(DisplayName = "Verify that RPC allow to scale processing and 3 running RPC servers can process 10 requests in 1 minute")]
        public async Task RpcScalabilityTest()
        {
            new RpcServer().DeclareQueue(QueueFour);
            new RpcServer().DeclareQueue(QueueFour);
            new RpcServer().DeclareQueue(QueueFour);

            var handler = new RequestReplyHandler();

            var tasks = new List<Task>();
           

            for (int i = 0; i < 5; i++)
            {
                tasks.Add(handler.InvokeAsync(QueueFour, TestMessageOne).WaitAsync(TimeSpan.FromSeconds(30)));
                tasks.Add(handler.InvokeAsync(QueueFour, TestMessageTwo).WaitAsync(TimeSpan.FromSeconds(30)));
            }
           
            DateTime start = DateTime.UtcNow;

            await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60));

            DateTime end = DateTime.UtcNow;
            TimeSpan timeDiff = (end - start);

            handler?.Messages?.Count.Should().Be(10, because: "Not all messages were processed");
            handler?.Messages?.All(m => m.Equals("HELLO, WORLD"));
        }       
    }
}
