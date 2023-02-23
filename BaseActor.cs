using RabbitMQ.Client;

namespace RabbitMqTest
{
    public abstract  class BaseActor : IDisposable
    {
        protected IModel Channel { get; private set; }
        IConnection _connection;

        public BaseActor()
        {
            var factory = new ConnectionFactory { HostName = "localhost" };
            _connection = factory.CreateConnection();
            Channel = _connection.CreateModel();
        }
        public virtual void Dispose()
        {
            _connection.Dispose();
            Channel.Dispose();           
        }
    }
}
