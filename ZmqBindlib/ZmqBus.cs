using NetMQ;
using System.Collections.Concurrent;

namespace MQBindlib
{

    /// <summary>
    /// 无中心广播
    /// </summary>
    public class ZmqBus
    {
         BlockingCollection<InerTopicMessage> queue = null;
        /// <summary>
        /// 次优先，字符串
        /// </summary>
        public event Action<string, string> StringReceived;



        /// <summary>
        /// 最优先，返回byte[]
        /// </summary>
        public event Action<string, byte[]> ByteReceived;

        private object _lock = new object();

        /// <summary>
        /// 广播端口
        /// </summary>
        public int Port { get; set; } = 5566;


        private Bus? bus;


        private bool isNeed = true;

        private bool isNeedTg = true;

        private void RunCactive()
        {
            lock (_lock)
            {

                if (bus == null)
                {
                    bus = Bus.Create(Port);
                }
            }
           
        }

        /// <summary>
        /// 接收数据
        /// </summary>
        private void Recvice()
        {
            lock (_lock)
            {
                if(queue!=null)
                {
                    return;
                }
                queue = new BlockingCollection<InerTopicMessage>();
                Thread thread = new Thread(() =>
                {

                    while (true)
                    {
                        var topic = bus.m_actor.ReceiveFrameString();

                        if (ByteReceived != null)
                        {
                            var data = bus.m_actor.ReceiveFrameBytes();
                            ByteReceived(topic, data);
                        }
                        if (StringReceived != null)
                        {
                            var msg = bus.m_actor.ReceiveFrameString();
                            StringReceived(topic, msg);
                        }
                        else
                        {
                            var msg = bus.m_actor.ReceiveFrameString();
                            queue.Add(new InerTopicMessage() { Topic = topic, Message = msg });
                        }
                    }

                });
                thread.Name = "zmqbusrecvice";
                thread.IsBackground = true;
                thread.Start();
            }
        }

        /// <summary>
        /// 发布数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="topic"></param>
        /// <param name="msg"></param>
        public void Publish<T>(string topic, T msg)
        {
            if (isNeed)
            {
                isNeed = false;
                RunCactive();
            }
            var p = Util.JSONSerializeObject(msg);
            bus.m_actor.SendMoreFrame(Bus.PublishCommand).SendMoreFrame(topic).SendFrame(p);
        }

        /// <summary>
        /// 订阅主题
        /// </summary>
        /// <param name="topic"></param>
       public void Subscribe(string topic)
        {
            if (isNeed)
            {
                isNeed = false;
                RunCactive();
            }
            if(isNeedTg)
            {
                isNeedTg= false;
                Recvice();
            }
            bus.Socket.Subscribe(topic);
        }
        /// <summary>
        /// 获取
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public TopicMessage<T> GetMsg<T>()
        {
            var result = queue.Take();

            T obj = Util.JSONDeserializeObject<T>(result.Message);

            return new TopicMessage<T>() { Topic = result.Topic, Message = obj };


        }
    }
}
