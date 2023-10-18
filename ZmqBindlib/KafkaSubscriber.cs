using Confluent.Kafka;

namespace MQBindlib
{
    /// <summary>
    /// 
    /// </summary>
    public class KafkaSubscriber
    {
        private IConsumer<string, string> _consumer;
        private IConsumer<string, byte[]> m_consumer;
        public KafkaSubscriber(string server = null)
        {
            if (string.IsNullOrEmpty(server))
            {
                server = "localhost:9092";
            }
            var config = new ConsumerConfig
            {
                GroupId = "MQBindlib",
                BootstrapServers = server,
                AutoOffsetReset = AutoOffsetReset.Latest
            };
            _consumer = new ConsumerBuilder<string, string>(config).Build();
          //  m_consumer=new ConsumerBuilder<string, byte[]>(config).Build();
          
            
        }

        public void Consume(Action<ConsumeResult<string, string>> action = null)
        {
            Task.Factory.StartNew(()=>
            {
                while (true)
               
                {
                    var consumerResult = _consumer.Consume(TimeSpan.FromSeconds(2));
                    if (consumerResult==null)
                    {
                        continue;
                    }
                    action?.Invoke(consumerResult);
                }
            });
        }

        public void ConsumeByte(Action<ConsumeResult<string, byte[]>> action = null)
        {
            
            var result = m_consumer.Consume(TimeSpan.FromSeconds(2));
            action?.Invoke(result);
        }

        public void Subscriber(string topic)
        {
            _consumer.Subscribe(topic);
          //  m_consumer.Subscribe(topic);
        }

    }
}
