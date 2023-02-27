using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Text;

namespace RabbitMqTest.RPC
{
    public class RpcClient : BaseActor
    {
        private string _queName;
        private string _replyQueueName;
        private readonly ConcurrentDictionary<string, TaskCompletionSource<string>> _callbackMapper = new();

        public RpcClient(string queueName) : base()
        {
            _queName = queueName;
            _replyQueueName = Channel.QueueDeclare().QueueName;
            var consumer = new EventingBasicConsumer(Channel);

            consumer.Received += (model, ea) =>
            {
                if (!_callbackMapper.TryRemove(ea.BasicProperties.CorrelationId, out var tcs))
                    return;
                var body = ea.Body.ToArray();
                var response = Encoding.UTF8.GetString(body);
                tcs.TrySetResult(response);
            };

            Channel.BasicConsume(consumer: consumer, queue: _replyQueueName, autoAck: true);
        }        

        public Task<string> CallAsync(string message, CancellationToken cancellationToken = default)
        {
            IBasicProperties props = Channel.CreateBasicProperties();
            var correlationId = Guid.NewGuid().ToString();
            props.CorrelationId = correlationId;
            props.ReplyTo = _replyQueueName;
            var messageBytes = Encoding.UTF8.GetBytes(message);
            var tcs = new TaskCompletionSource<string>();
            _callbackMapper.TryAdd(correlationId, tcs);

            Channel.BasicPublish(exchange: string.Empty,
                                 routingKey: _queName,
                                 basicProperties: props,
                                 body: messageBytes);

            cancellationToken.Register(() => _callbackMapper.TryRemove(correlationId, out _));
            return tcs.Task;
        }
    }
}
