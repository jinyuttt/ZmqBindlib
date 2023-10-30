using Confluent.Kafka;
using System.Text;

namespace MQBindlib
{
    /// <summary>
    /// kafka订阅
    /// </summary>
    public class KafkaSubscriber
    {
      
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
          
           m_consumer=new ConsumerBuilder<string, byte[]>(config).Build();
          
            
        }

        public void Consume(Action<ConsumeResult<string, string>> action = null)
        {
            Task.Factory.StartNew(()=>
            {
                while (true)
               
                {
                    var consumerResult = m_consumer.Consume(TimeSpan.FromSeconds(2));
                    if (consumerResult==null)
                    {
                        continue;
                    }
                    ConsumeResult<string, string> result = new ConsumeResult<string, string>();
                    result.TopicPartitionOffset = consumerResult.TopicPartitionOffset;
                    Message<string, string> msg = new Message<string, string>();
                    msg.Key = consumerResult.Key;
                    msg.Value = Encoding.UTF8.GetString(consumerResult.Value);
                    msg.Headers = consumerResult.Headers;
                    msg.Timestamp = consumerResult.Timestamp;
                    result.Message =msg;
                    action?.Invoke(result);
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
          
           m_consumer.Subscribe(topic);
        }

    }
}
