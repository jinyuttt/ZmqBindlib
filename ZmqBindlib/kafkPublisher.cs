using Confluent.Kafka;

namespace MQBindlib
{
    public class KafkaPublisher
    {
        private ProducerConfig _config = new ProducerConfig();
        private IProducer<string, string> _producer;
        private IProducer<string, byte[]> m_producer;
        public KafkaPublisher(string server = null)
        {
            if (string.IsNullOrEmpty(server))
            {
                //这里可以添加更多的Kafka集群，比如
                //server=" server ="192.168.1.129:9092,192.168.1.133:9092,192.168.1.134:9092";";                   
                server = "localhost:9092";

            }
            _config.BootstrapServers = server;
            
            _producer = new ProducerBuilder<string, string>(_config).Build();
          //  m_producer = new ProducerBuilder<string, byte[]>(_config).Build();

        }

        public async Task<DeliveryResult<string, string>> ProduceAsync(string topic, Message<string, string> message)
        {
            return await _producer.ProduceAsync(topic, message);
        }

        public void  Push(string topic, Message<string, string> message)
        {
            _producer.Produce(topic, message);
        }
        public void Push(string topic, string message)
        {
            _producer.Produce(topic, new Message<string, string> { Value = message });
        }

        public void Push(string topic, byte[] message)
        {
            m_producer.Produce(topic, new Message<string, byte[]> { Value = message });
        }

        public void Push<T>(string topic,T obj)
        {
           var message=  Util.JSONSerializeObject(obj);
            _producer.Produce(topic, new Message<string, string> { Value = message });
        }
    }
}
