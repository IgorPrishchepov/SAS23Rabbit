using RabbitMQ.Client;
using System.Security.Cryptography;
using System.Text;

namespace RabbitMqTest
{
    public class ProducerTranslator : BaseActor
    {
        public void PublishToQueueDefaultExchange(string queueName, string? message = default)
        {
            var msg = Encoding.UTF8.GetBytes(message.ToUpper());

            var processingTime = RandomNumberGenerator.GetInt32(100, 10000);
            Thread.Sleep(processingTime);

            Channel.BasicPublish(exchange: string.Empty, routingKey: queueName, body: msg);            
        }


    }
}
