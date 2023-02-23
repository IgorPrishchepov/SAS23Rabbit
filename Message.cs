namespace RabbitMqTest
{
    public struct Message
    {
        public string Queue { get; private set; }
        public string Body { get; private set; }
        public Message(string queueName, string messageBody)
        {
            Queue = queueName;
            Body = messageBody;
        }
    }
}
