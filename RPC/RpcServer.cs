using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Security.Cryptography;
using System.Text;

namespace RabbitMqTest.RPC
{
    public class RpcServer : BaseActor
    {
        public void DeclareQueue(string queueName)
        {
            Channel.QueueDeclare(queue: queueName,
                     durable: false,
                     exclusive: false,
                     autoDelete: true,
                     arguments: null);

            Channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

            var consumer = new EventingBasicConsumer(Channel);

            Channel.BasicConsume(queue: queueName,
                     autoAck: false,
                     consumer: consumer);

            consumer.Received += (model, ea) =>
            {
                string response = string.Empty;

                var body = ea.Body.ToArray();
                var props = ea.BasicProperties;
                var replyProps = Channel.CreateBasicProperties();
                replyProps.CorrelationId = props.CorrelationId;

                var message = Encoding.UTF8.GetString(body);
                response = ProcessMessage(message);

                var responseBytes = Encoding.UTF8.GetBytes(response);
                Channel.BasicPublish(exchange: string.Empty,
                                     routingKey: props.ReplyTo,
                                     basicProperties: replyProps,
                                     body: responseBytes);
                Channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

            };
        }

        private static string ProcessMessage(string message)
        {
            var processingTime = RandomNumberGenerator.GetInt32(100, 10000);
            Thread.Sleep(processingTime);
            return message.ToUpper();
        }
    }
}
