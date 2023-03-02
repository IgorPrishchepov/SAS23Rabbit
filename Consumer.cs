using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Text;

namespace RabbitMqTest
{
    public class Consumer : BaseActor
    {
        public ConcurrentBag<Message>? Messages { get; private set; }

        public Consumer()
        {
            Messages = new ConcurrentBag<Message>();
        }

        public void DeclareQueue(
            string queueName,
            bool ack = false,
            int processingDelay = 0)
        {
            Task.Run(() => 
            {
                Channel.QueueDeclare(queue: queueName,
                        durable: false,
                        exclusive: false,
                        autoDelete: true,
                        arguments: null);
                var consumer = new EventingBasicConsumer(Channel);

                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Thread.Sleep(processingDelay);
                    Messages?.Add(new Message(queueName, message));

                    if (ack)
                    {
                        Channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    }                    
                };               

                Channel.BasicConsume(queue: queueName,
                     autoAck: !ack,
                     consumer: consumer);
            });           
        }

        public void DeclareDirectExchange(string queueName, string exchange, string routingKey)
        {
            Channel.ExchangeDeclare(exchange, ExchangeType.Direct);
            Channel.QueueBind(queueName, exchange, routingKey);
            Task.Run(() =>
            {               
                var consumer = new EventingBasicConsumer(Channel);

                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Messages?.Add(new Message(queueName, message));                   
                };

                Channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
            });
        }

        public void DeclareTopicExchange(string queueName, string exchange, string routingKey)
        {
            Channel.ExchangeDeclare(exchange, ExchangeType.Topic);
            Channel.QueueBind(queueName, exchange, routingKey);
            Task.Run(() =>
            {
                var consumer = new EventingBasicConsumer(Channel);

                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Messages?.Add(new Message(queueName, message));
                };

                Channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
            });
        }

        public override void Dispose()
        {
            base.Dispose();
            Messages.Clear();
        }
    }
}
