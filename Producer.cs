using RabbitMQ.Client;

namespace RabbitMqTest
{
    public class Producer : BaseActor
    {  

        public void DeclareQueue(string queueName)
        {
            Channel.QueueDeclare(queue: queueName,
                         durable: false,
                         exclusive: false,
                         autoDelete: true,
                         arguments: null);
        }

        public void PublishToQueueDefaultExchange(string queueName, int numberOfMessages = 1, int frequencyMilliseconds = 0, ReadOnlyMemory<byte> message = default)
        {
            for (int i = 0; i < numberOfMessages; i++)
            {
               Channel.BasicPublish(exchange: string.Empty, routingKey: queueName, body: message);
               Thread.Sleep(frequencyMilliseconds);
            }            
        }

        public void PublishToQueueDirectExchange(string queueName, string exchange, string routingKey, int numberOfMessages = 1, int frequencyMilliseconds = 0, ReadOnlyMemory<byte> message = default)
        {
            Channel.ExchangeDeclare(exchange: exchange, type: ExchangeType.Direct);

            for (int i = 0; i < numberOfMessages; i++)
            {
                Channel.BasicPublish(exchange: exchange, routingKey: routingKey, body: message);
                Thread.Sleep(frequencyMilliseconds);
            }
        }

        public void PublishToQueueTopicExchange(string queueName, string exchange, string routingKey, int numberOfMessages = 1, int frequencyMilliseconds = 0, ReadOnlyMemory<byte> message = default)
        {
            Channel.ExchangeDeclare(exchange: exchange, type: ExchangeType.Topic);

            for (int i = 0; i < numberOfMessages; i++)
            {
                Channel.BasicPublish(exchange: exchange, routingKey: routingKey, body: message);
                Thread.Sleep(frequencyMilliseconds);
            }

        }
    }
}
