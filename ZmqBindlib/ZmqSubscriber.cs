using NetMQ;
using NetMQ.Sockets;
using System.Collections.Concurrent;

namespace ZmqBindlib
{
    /// <summary>
    /// 订阅
    /// </summary>
    public class ZmqSubscriber
    {
        SubscriberSocket subscriber = null;
        readonly BlockingCollection<InerTopicMessage> queue = new BlockingCollection<InerTopicMessage>();

        /// <summary>
        /// 订阅地址
        /// </summary>
        public string[]? Address { get; set; }

        /// <summary>
        /// 次优先，字符串
        /// </summary>
        public event Action<string,string> StringReceived;



        /// <summary>
        /// 最优先，返回byte[]
        /// </summary>
        public event Action<string,byte[]> ByteReceived;

        public ZmqSubscriber() { }

        public ZmqSubscriber(string[] address)
        {
            this.Address = address;
        }

        private void Recvice()
        {
            while (true)
            {

                var client = subscriber.ReceiveFrameString();
                var topic = subscriber.ReceiveFrameString();
                if(client!=null)
                {
                    Console.WriteLine(client);
                }
                if (ByteReceived!=null)
                {
                    var data = subscriber.ReceiveFrameBytes();
                    ByteReceived(topic, data);
                }
                if (StringReceived != null)
                {
                    var msg = subscriber.ReceiveFrameString();
                    StringReceived(topic, msg);
                }
                else
                {
                    var msg = subscriber.ReceiveFrameString();
                    queue.Add(new InerTopicMessage() { Topic = topic, Message = msg });
                }
            }

        }

        /// <summary>
        /// 订阅主题
        /// </summary>
        /// <param name="topic"></param>
        public void Subscribe(string topic)
        {
            if (this.subscriber == null)
            {
                this.subscriber = new SubscriberSocket();
                subscriber.Options.ReceiveHighWatermark = 0;
                subscriber.Options.TcpKeepalive = true;
                subscriber.Options.HeartbeatInterval = new TimeSpan(10000);
                subscriber.Options.HeartbeatTimeout = new TimeSpan(1000);
                subscriber.Options.HeartbeatTtl = new TimeSpan(2000);
                foreach (string s in Address)
                {
                    subscriber.Connect(s);
                }
                Thread thread = new Thread(new ThreadStart(Recvice));
                thread.Name = "Subscribe";
                thread.IsBackground = true;
                thread.Start();
            }
            subscriber.Subscribe(topic);
        }

        /// <summary>
        /// 取消订阅
        /// </summary>
        /// <param name="topic"></param>
        public void UnSubscribe(string topic)
        {
           
            subscriber.Unsubscribe(topic);
        }

        /// <summary>
        /// 获取
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public TopicMessage<T> GetMsg<T>()
        {
            var result = queue.Take();

            T obj= Util.JSONDeserializeObject<T>(result.Message);

            return new TopicMessage<T>() { Topic=result.Topic, Message = obj };


        }
    }
    
}
