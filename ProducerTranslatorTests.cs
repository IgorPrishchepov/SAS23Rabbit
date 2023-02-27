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
    public class ProducerTranslatorTests
    {
        const string QueueFour = "queue_4";
        const string TestMessageOne = "Hello, world";
        const string TestMessageTwo = "hello, World";

        [Fact(DisplayName = "Rpc test")]
        public async Task RpcTest()
        {
            new RpcServer().DeclareQueue(QueueFour);
            new RpcServer().DeclareQueue(QueueFour);
            new RpcServer().DeclareQueue(QueueFour);

            var handler = new RequestReplyHandler();

            var tasks = new List<Task>();
           

            for (int i = 0; i < 10; i++)
            {
                tasks.Add(handler.InvokeAsync(QueueFour, TestMessageOne).WaitAsync(TimeSpan.FromSeconds(30)));
            }




            /*var tasks = new List<Task>
            {
                handler.InvokeAsync(QueueFour, TestMessageOne),
              //  handler.InvokeAsync(QueueFour, TestMessageTwo)
            };*/
            DateTime start = DateTime.UtcNow;

            await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60));

            DateTime end = DateTime.UtcNow;
            TimeSpan timeDiff = (end - start);

         //   handler?.Messages?.Count.Should().Be(5, because: "Not all messages were processed");
        }













        [Fact(DisplayName = "Verify that 1 Consumer receives all messages produced by 1 producer converted to upper case")]
        public async Task OneProducerOneConsumerTest()
        {

            using var producer = new ProducerTranslator();
            producer.DeclareQueue(QueueFour);

            using var consumer = new Consumer();
            consumer.DeclareQueue(QueueFour);

            producer.PublishToQueueDefaultExchange(QueueFour, message: "Hello, world");

            await TestUtilities.WaitUntil(() => consumer.Messages?.Count == 1);

            consumer?.Messages?.All(m => m.Queue.Equals(QueueFour)).Should().BeTrue(because: $"Not all messages belong to {QueueFour} queu");
            consumer?.Messages?.All(m => m.Body.Equals(TestMessageOne.ToUpper())).Should().BeTrue(because: $"Not all messages have correct data");
        }

        [Fact(DisplayName = "Verify that 2 Consumers receives all messages produced by 1 producer")]
        public async Task OneProducerTwoConsumersTest()
        {

            using var producer = new ProducerTranslator();
            producer.DeclareQueue(QueueFour);

            using var consumerOne = new Consumer();
            consumerOne.DeclareQueue(QueueFour);

            using var consumerTwo = new Consumer();
            consumerTwo.DeclareQueue(QueueFour);

            DateTime start = DateTime.UtcNow;



            _ = Task.Run(() =>
            {
                for (int i = 0; i < 5; i++)
                {
                    producer.PublishToQueueDefaultExchange(QueueFour, message: TestMessageOne);
                    producer.PublishToQueueDefaultExchange(QueueFour, message: TestMessageTwo);
                }
            }
            );

            await TestUtilities.WaitUntil(() => consumerOne.Messages?.Count + consumerTwo.Messages.Count == 10, timeout: 120000);

            DateTime end = DateTime.UtcNow;
            TimeSpan timeDiff = end - start;
            Convert.ToInt32(timeDiff.TotalSeconds).Should().BeLessThan(120, because: "All messages must be consumed in less than 2 minutes");

            consumerOne?.Messages?.All(m => m.Body.Equals(TestMessageOne.ToUpper())).Should().BeTrue(because: $"Not all messages have correct data");
            consumerTwo?.Messages?.All(m => m.Body.Equals(TestMessageOne.ToUpper())).Should().BeTrue(because: $"Not all messages have correct data");
        }
    }
}
