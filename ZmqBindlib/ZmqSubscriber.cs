using NetMQ;
using NetMQ.Sockets;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace MQBindlib
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
        /// 中心模式，并且主从
        /// </summary>
        public bool IsDDS {get; set; }

        /// <summary>
        /// 次优先，字符串
        /// </summary>
        public event Action<string,string> StringReceived;



        /// <summary>
        /// 最优先，返回byte[]
        /// </summary>
        public event Action<string,byte[]> ByteReceived;

        private List<string> topics=new List<string>();

        List<ClusterNode> lstNode=new List<ClusterNode>();

        DateTime fulshTime=DateTime.Now;

        private readonly TimeSpan m_deadNodeTimeout = TimeSpan.FromSeconds(10);

     

        public ZmqSubscriber() {
          
        }

        public ZmqSubscriber(string[] address)
        {
          
            this.Address = address;
           
        }

        private void Update()
        {
            Thread up = new Thread(p =>
            {
                while (true)
                {


                    Thread.Sleep(1000);

                    if (lstNode != null)
                    {
                        var master = lstNode.Find(p => p.IsMaster);
                        if (master != null)
                        {
                            if (master.Address != Address[0] || DateTime.Now > fulshTime + m_deadNodeTimeout)
                            {

                                foreach (string tmp in topics)
                                {
                                    //防止切换时异常，尤其是网络异常
                                    subscriber.Unsubscribe(tmp);
                                }
                                subscriber.Disconnect(Address[0]);
                                if (Address[0] == master.Address)
                                {
                                    //超时，切换地址
                                    var tmp = lstNode.Where(p => p.Id != master.Id).OrderByDescending(p => p.Id).FirstOrDefault();
                                    if (tmp != null)
                                    {
                                        try
                                        {

                                            subscriber.Connect(tmp.Address);
                                            subscriber.Subscribe(ConstString.ReqCluster);
                                            Address[0] = tmp.Address;
                                            Console.WriteLine("超时切换:" + tmp.Address);

                                            foreach (string tp in topics)
                                            {
                                                subscriber.Subscribe(tp);
                                            }
                                            tmp.IsMaster = true;
                                            master.IsMaster = false;
                                        }
                                        catch(Exception ex)
                                        {
                                           Console.WriteLine(ex);
                                        }
                                    }
                                    Thread.Sleep(m_deadNodeTimeout);
                                }
                                else
                                {
                                    //master切换
                                   
                                    subscriber.Connect(master.Address);
                                    subscriber.Subscribe(ConstString.ReqCluster);
                                    Address[0] = master.Address;
                                    Console.WriteLine("主从切换:" + master.Address);

                                    foreach (string tp in topics)
                                    {
                                        subscriber.Subscribe(tp);
                                    }
                                }


                            }
                        }
                    }
                }
            });
            up.Start();
        }
       
        private void Recvice()
        {
            while (true)
            {

                //var client = subscriber.ReceiveFrameString();
                var topic = subscriber.ReceiveFrameString();
                //if(client!=null)
                //{
                //    Console.WriteLine(client);
                //}
                if(ConstString.ReqCluster ==topic)
                {
                    string msg = subscriber.ReceiveFrameString();
                    List<ClusterNode> lst = Util.JSONDeserializeObject<List<ClusterNode>>(msg);
                    lstNode = lst;
                    fulshTime=DateTime.Now;
                  
                    continue;
                }
                if(ConstString.PubPublisher ==topic)
                {
                    subscriber.ReceiveFrameString();
                    continue;//过滤
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
                
                Thread thread = new Thread(Recvice);
                thread.Name = "Subscribe";
                thread.IsBackground = true;
                thread.Start();
                if(IsDDS)
                {
                    subscriber.Subscribe(ConstString.ReqCluster);
                    Update();
                }
            }
            subscriber.Subscribe(topic);
            if(!topics.Contains(topic))
            {
                topics.Add(topic);
            }
        }

        /// <summary>
        /// 取消订阅
        /// </summary>
        /// <param name="topic"></param>
        public void UnSubscribe(string topic)
        {
           
            subscriber.Unsubscribe(topic);
        }

        public void Disconnect(string address)

        {
            if (subscriber != null)
                subscriber.Disconnect(address);
        }

        public void Connect(string address)

        {
            if(subscriber!=null)
            subscriber.Connect(address);
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
