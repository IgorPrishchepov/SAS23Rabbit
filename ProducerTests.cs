using FluentAssertions;
using System.Text;

namespace RabbitMqTest
{
    public class ProducerTests
    {
        const string QueueOne = "queue_1";
        const string QueueTwo = "queue_2";
        const string QueueThree = "queue_3";
        const string TestMessageOne = "Hello from XUnit";
        const string TestMessageTwo = "Direct: Hello from XUnit";

        [Fact(DisplayName = "Verify that 1 Consumer receives all messages produced by 1 producer by Default exchange")]
        public async Task OneProducerOneConsumerTest()
        {
            using var consumer = new Consumer();
            consumer.DeclareQueue(QueueOne);

            using var producer = new Producer();           
            producer.DeclareQueue(QueueOne);
           
            var msg = Encoding.UTF8.GetBytes(TestMessageOne);
            producer.PublishToQueueDefaultExchange(QueueOne, numberOfMessages:5, frequencyMilliseconds: 5000, message: msg); ;

            await TestUtilities.WaitUntil(() => consumer.Messages?.Count == 5 );

            consumer?.Messages?.All(m => m.Queue.Equals(QueueOne)).Should().BeTrue(because: $"Not all messages belong to {QueueOne} queu");
            consumer?.Messages?.All(m => m.Body.Equals(TestMessageOne)).Should().BeTrue(because: $"Not all messages have correct data");

        }

        [Fact(DisplayName = "Verify that 2 Consumers receive all messages produced by 1 producer of the same queue by Default exchange")]
        public async Task TwoConsumersSameQueueTest()
        {
            using var consumerOne = new Consumer();
            consumerOne.DeclareQueue(QueueOne);

            using var consumerTwo = new Consumer();
            consumerTwo.DeclareQueue(QueueOne);

            using var producer = new Producer();
            producer.DeclareQueue(QueueOne);

            var msg = Encoding.UTF8.GetBytes(TestMessageOne);
            producer.PublishToQueueDefaultExchange(QueueOne, numberOfMessages: 5, frequencyMilliseconds: 5000, message: msg); ;

            await TestUtilities.WaitUntil(() => (consumerOne.Messages?.Count + (consumerTwo.Messages?.Count) == 5));

            (!consumerOne?.Messages?.IsEmpty).Should().BeTrue(because: "Comnsumer One must consume part of messages");
            (!consumerTwo?.Messages?.IsEmpty).Should().BeTrue(because: "Comnsumer Two must consume part of messages");

            consumerOne?.Messages?.All(m => m.Queue.Equals(QueueOne)).Should().BeTrue(because: $"Not all messages belong to {QueueOne} queu");
            consumerTwo?.Messages?.All(m => m.Queue.Equals(QueueOne)).Should().BeTrue(because: $"Not all messages belong to {QueueOne} queu");
            consumerOne?.Messages?.All(m => m.Body.Equals(TestMessageOne)).Should().BeTrue(because: $"Not all messages have correct data");
            consumerTwo?.Messages?.All(m => m.Body.Equals(TestMessageOne)).Should().BeTrue(because: $"Not all messages have correct data");
        }

        [Fact(DisplayName = "Verify that 2 Consumers receive all messages produced by 1 producer of the different queues by Direct exchange")]
        public async Task TwoConsumersDifferentQueuesTest()
        {
            using var consumerOne = new Consumer();
            consumerOne.DeclareQueue(QueueOne);

            using var consumerTwo = new Consumer();
            consumerTwo.DeclareQueue(QueueTwo);

            using var producer = new Producer();
            producer.DeclareQueue(QueueOne);
            producer.DeclareQueue(QueueTwo);

            var msg = Encoding.UTF8.GetBytes(TestMessageOne);
            producer.PublishToQueueDefaultExchange(QueueOne, numberOfMessages: 5, frequencyMilliseconds: 5000, message: msg);
                        
            producer.PublishToQueueDefaultExchange(QueueTwo, numberOfMessages: 5, frequencyMilliseconds: 5000, message: msg);

            await TestUtilities.WaitUntil(() => (consumerOne.Messages?.Count + (consumerTwo.Messages?.Count) == 10));

            consumerOne?.Messages?.All(m => m.Queue.Equals(QueueOne)).Should().BeTrue(because: $"Not all messages belong to {QueueOne} queu");
            consumerTwo?.Messages?.All(m => m.Queue.Equals(QueueTwo)).Should().BeTrue(because: $"Not all messages belong to {QueueTwo} queu");
            consumerOne?.Messages?.All(m => m.Body.Equals(TestMessageOne)).Should().BeTrue(because: $"Not all messages have correct data");
            consumerTwo?.Messages?.All(m => m.Body.Equals(TestMessageOne)).Should().BeTrue(because: $"Not all messages have correct data");
        }

        [Fact(DisplayName = "Verify that 1 Consumer receives all messages produced by 1 producer of the different queues by Default exchange")]
        public async Task OneConsumerTwoQueuesTest()
        {
            using var consumerOne = new Consumer();
            consumerOne.DeclareQueue(QueueOne);            
            consumerOne.DeclareQueue(QueueTwo);

            using var producer = new Producer();
            producer.DeclareQueue(QueueOne);
            producer.DeclareQueue(QueueTwo);

            var msg = Encoding.UTF8.GetBytes(TestMessageOne);
            producer.PublishToQueueDefaultExchange(QueueOne, numberOfMessages: 5, frequencyMilliseconds: 2000, message: msg);

            producer.PublishToQueueDefaultExchange(QueueTwo, numberOfMessages: 5, frequencyMilliseconds: 3000, message: msg);

            await TestUtilities.WaitUntil(() => (consumerOne.Messages?.Count == 10));

            consumerOne?.Messages?.Where(m => m.Queue.Equals(QueueOne)).Count().Should().Be(5, because: $"Not all messages of {QueueOne} were consumed");
            consumerOne?.Messages?.Where(m => m.Queue.Equals(QueueTwo)).Count().Should().Be(5, because: $"Not all messages of {QueueTwo} were consumed");        
        }

        [Fact(DisplayName = "Verify that second Consumer receives message that was not completely processed by first consumer and was not acknowledged")]
        public async Task TwoConsumersOneQueueWithAcknowledgementTest()
        {
            using var consumerOne = new Consumer();
            consumerOne.DeclareQueue(QueueOne, ack: true, processingDelay: 20000);
            using var producer = new Producer();
            producer.DeclareQueue(QueueOne);

            var msg = Encoding.UTF8.GetBytes(TestMessageOne);
            producer.PublishToQueueDefaultExchange(QueueOne, message: msg);

            using var consumerTwo = new Consumer();
            consumerTwo.DeclareQueue(QueueOne);

            consumerOne.Dispose();            

            await TestUtilities.WaitUntil(() => consumerTwo.Messages?.Count != 0);

            consumerOne.Messages.Should().BeEmpty(because: "First consumer must not be able to process the message");
            consumerTwo?.Messages?.All(m => m.Queue.Equals(QueueOne)).Should().BeTrue(because: $"Not all messages belong to {QueueOne} queu");
            consumerTwo?.Messages?.All(m => m.Body.Equals(TestMessageOne)).Should().BeTrue(because: $"Not all messages have correct data");
        }

        [Fact(DisplayName = "Verify that Consumer receives Direct exchange messages for specific routing key produced by 1 producer within one queue")]
        public async Task OneProducerTwoConsumersDirectExchangeTest()
        {
            var exchange = "direct_exchange";
            var routingKey = "direct_routing";

            using var consumer = new Consumer();
            consumer.DeclareDirectExchange(QueueOne, exchange, routingKey);

            using var producer = new Producer();
            producer.DeclareQueue(QueueOne);

            var msgOne = Encoding.UTF8.GetBytes(TestMessageOne);
            producer.PublishToQueueDirectExchange(
                queueName: QueueOne,
                exchange: "exchange_1",
                routingKey: "routing_1",
                numberOfMessages: 5,
                frequencyMilliseconds: 2000,
                message: msgOne);

            var msgTwo = Encoding.UTF8.GetBytes(TestMessageTwo);
            producer.PublishToQueueDirectExchange(
                queueName: QueueOne,
                exchange: exchange,
                routingKey: routingKey,
                numberOfMessages: 3,
                frequencyMilliseconds: 5000,
                message: msgTwo);                    
            
            await TestUtilities.WaitUntil(() => consumer.Messages?.Count == 3);

            consumer?.Messages?.All(m => m.Queue.Equals(QueueOne)).Should().BeTrue(because: $"Not all messages belong to {QueueOne} queu");
            consumer?.Messages?.All(m => m.Body.Equals(TestMessageTwo)).Should().BeTrue(because: $"Not all messages have correct data");
        }

        [Fact(DisplayName = "Verify that Consumer receives Topic exchange messages for specific routing keys produced by 1 producer")]
        public async Task OneProducerOneConsumerTopicExchangeTest()
        {
            var exchange = "topic_exchange";
            var routingKeyCritical = "critical.message";
            var routingKeyMajor = "major.message";

            var keyValue = new Dictionary<string, string>()
            {
                { routingKeyCritical, "A critical kernel error"},
                { routingKeyMajor, "A major kernel error"}
            };

            using var producer = new Producer();
            producer.DeclareQueue(QueueThree);

            using var consumerOne = new Consumer();
            consumerOne.DeclareTopicExchange(QueueThree, exchange, "critical.*");           

            var msg = Encoding.UTF8.GetBytes(keyValue[routingKeyCritical]);
            producer.PublishToQueueTopicExchange(
                queueName: QueueThree,
                exchange: exchange,
                routingKey: routingKeyCritical,
                numberOfMessages: 3,
                frequencyMilliseconds: 2000,
                message: msg);

            msg = Encoding.UTF8.GetBytes(keyValue[routingKeyMajor]);
            producer.PublishToQueueTopicExchange(
                queueName: QueueThree,
                exchange: exchange,
                routingKey: routingKeyMajor,
                numberOfMessages: 2,
                frequencyMilliseconds: 2000,
                message: msg);

            await TestUtilities.WaitUntil(() => consumerOne.Messages?.Count == 3);

            consumerOne?.Messages?.All(m => m.Queue.Equals(QueueThree)).Should().BeTrue(because: $"Not all messages belong to {QueueOne} queue");
            consumerOne?.Messages?.All(m => m.Body.Equals(keyValue[routingKeyCritical])).Should().BeTrue(because: $"Not all messages have correct data");
        }
    }
}