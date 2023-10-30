using Confluent.Kafka;
using System.Text;

namespace MQBindlib
{

    /// <summary>
    /// Kafka发布
    /// </summary>
    public class KafkaPublisher
    {
        private ProducerConfig _config = new ProducerConfig();

        private IProducer<string, byte[]> m_producer;
        public KafkaPublisher(string server = null)
        {
            if (string.IsNullOrEmpty(server))
            {
                //这里可以添加更多的Kafka高可用，比如
                //server=" server ="192.168.1.129:9092,192.168.1.133:9092,192.168.1.134:9092";";                   
                server = "localhost:9092";

            }
            _config.BootstrapServers = server;
            m_producer = new ProducerBuilder<string, byte[]>(_config).Build();

        }

        /// <summary>
        /// 异步发布数据
        /// </summary>
        /// <param name="topic">主题</param>
        /// <param name="message">数据</param>
        /// <returns></returns>
        public async Task<DeliveryResult<string, string>> ProduceAsync(string topic, Message<string, string> message)
        {
            var msg = Encoding.UTF8.GetBytes(message.Value);
            Message<string, byte[]> cur = new Message<string, byte[]>();
            cur.Key = message.Key;
            cur.Value = msg;
            cur.Headers = message.Headers;
            cur.Timestamp = message.Timestamp;
            var ret = await m_producer.ProduceAsync(topic, cur);

            DeliveryResult<string, string> result = new DeliveryResult<string, string>();
            result.Key = ret.Key;
            result.Value = Encoding.UTF8.GetString(ret.Value);
            result.Status = ret.Status;
            return result;
        }

        /// <summary>
        /// 发布数据
        /// </summary>
        /// <param name="topic">主题</param>
        /// <param name="message">数据</param>
        public void Push(string topic, Message<string, string> message)
        {
            var msg = Encoding.UTF8.GetBytes(message.Value);
            Message<string, byte[]> cur = new Message<string, byte[]>();
            cur.Key = message.Key;
            cur.Value = msg;
            cur.Headers = message.Headers;
            cur.Timestamp = message.Timestamp;
            m_producer.Produce(topic, cur);
        }

        /// <summary>
        /// 发布数据
        /// </summary>
        /// <param name="topic">主题</param>
        /// <param name="message">数据</param>
        public void Push(string topic, string message)
        {
            m_producer.Produce(topic, new Message<string, byte[]> { Value = Encoding.UTF8.GetBytes(message) });
        }

        /// <summary>
        /// 发布数据
        /// </summary>
        /// <param name="topic">主题</param>
        /// <param name="message">数据</param>
        public void Push(string topic, byte[] message)
        {
            m_producer.Produce(topic, new Message<string, byte[]> { Value = message });
        }

        /// <summary>
        /// 发布数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="topic">主题</param>
        /// <param name="obj">数据</param>
        public void Push<T>(string topic, T obj)
        {
            var message = Util.JSONSerializeObject(obj);
            var msg = Encoding.UTF8.GetBytes(message);
            m_producer.Produce(topic, new Message<string, byte[]> { Value = msg });
        }
    }
}
