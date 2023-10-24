using NetMQ;
using NetMQ.Sockets;
using System.Collections.Concurrent;

namespace MQBindlib
{
    public class ZmqSubscriberGroup
    {
        PullSocket  subscriber = null;
        readonly BlockingCollection<InerTopicMessage> queue = new BlockingCollection<InerTopicMessage>();

        readonly List<string> lst=new List<string>();

        /// <summary>
        /// 次优先，字符串
        /// </summary>
        public event Action<string, string> StringReceived;



        /// <summary>
        /// 最优先，返回byte[]
        /// </summary>
        public event Action<string, byte[]> ByteReceived;

        /// <summary>
        /// 订阅地址
        /// </summary>
        public string Address { get; set; }

        public string Indenty { get; set; } = "mqbindlib";

        /// <summary>
        /// 获取数据
        /// </summary>
        public DataModel DataOffset=DataModel.Latest;

        /// <summary>
        /// 有高可用部署
        /// </summary>
        public bool IsDDS { get; set; }

        private bool IsConnected = true;

        private List<string> topics = new List<string>();
        List<ClusterNode> lstNode = new List<ClusterNode>();

        DateTime fulshTime = DateTime.Now;

        private readonly TimeSpan m_deadNodeTimeout = TimeSpan.FromSeconds(10);

      /// <summary>
      /// 更新数据和中心
      /// </summary>
        private void Update()
        {
            Thread up = new Thread(p =>
            {

             //   this.Subscribe(ConstString.ReqCluster);
                while (true)
                {
                    Thread.Sleep(1000);

                    if (lstNode != null)
                    {
                        var master = lstNode.Find(p => p.IsMaster);
                        if (master != null)
                        {
                            if (master.Address != Address || DateTime.Now > fulshTime + m_deadNodeTimeout)
                            {
                                try
                                {
                                    subscriber.Disconnect(Address);
                                }
                                catch(Exception ex)
                                {
                                    Console.WriteLine(ex.ToString());
                                }
                                if (Address == master.Address)
                                {
                                    //超时，切换地址
                                    var tmp = lstNode.Where(p => p.Id != master.Id).OrderByDescending(p => p.Id).FirstOrDefault();
                                    if (tmp != null)
                                    {
                                        subscriber.Connect(tmp.Address);

                                        Address = tmp.Address;
                                        //重新订阅
                                        this.Subscribe(ConstString.ReqCluster);
                                        tmp.IsMaster = true;
                                        master.IsMaster = false;
                                        Console.WriteLine("sub切换:" + tmp.Address);

                                        foreach (string tp in topics)
                                        {
                                            this.Subscribe(tp);
                                        }
                                        Thread.Sleep(m_deadNodeTimeout);
                                    }
                                }
                                else
                                {
                                    //master切换


                                    subscriber.Connect(master.Address);
                                    this.Subscribe(ConstString.ReqCluster);
                                    Address = master.Address;
                                    Console.WriteLine("sub切换:" + master.Address);

                                    foreach (string tp in topics)
                                    {
                                        this.Subscribe(tp);
                                    }
                                }


                            }
                        }
                    }
                }
            });
            up.Start();
        }

        private void Recvice(string address)
        {
           
            lock (this)
            {

                if (subscriber != null) { subscriber.Connect(address);return; }
                if(IsDDS)
                {
                    Update();
                }    
                subscriber = new PullSocket();
                subscriber.Connect(address);
                Thread rec = new Thread(p =>
                {
                    while (IsConnected)
                    {

                        try
                        {
                            var topic = subscriber.ReceiveFrameString();
                            if (ConstString.ReqCluster == topic)
                            {
                                string msg = subscriber.ReceiveFrameString();
                                List<ClusterNode> lst = Util.JSONDeserializeObject<List<ClusterNode>>(msg);
                                lstNode = lst;
                                fulshTime = DateTime.Now;
                                continue;
                            }

                            if (ByteReceived != null)
                            {
                                var data = subscriber.ReceiveFrameBytes();
                                if (lst.Contains(topic))
                                {
                                    continue;
                                }
                                ByteReceived(topic, data);
                            }
                            if (StringReceived != null)
                            {
                                var msg = subscriber.ReceiveFrameString();
                                if (lst.Contains(topic))
                                {
                                    continue;
                                }
                                StringReceived(topic, msg);
                            }
                            else
                            {
                                var msg = subscriber.ReceiveFrameString();
                                if (lst.Contains(topic))
                                {
                                    continue;
                                }
                                queue.Add(new InerTopicMessage() { Topic = topic, Message = msg });
                            }
                        }
                        catch(Exception ex) {

                        }

                    }
                    Console.WriteLine("exit");
                });
                rec.Name = "zmqGroup";
                rec.IsBackground = true;
                rec.Start();
            }

        }
       
        /// <summary>
        /// 订阅主题
        /// </summary>
        /// <param name="topic"></param>
        public void Subscribe(string topic)
        {
            lst.Remove(topic); 
            if (!topics.Contains(topic))
            {
                topics.Add(topic);
            }
            using(var socket = new RequestSocket(Address))
            {
           
                socket.SendMoreFrame(topic).SendMoreFrame(Indenty).SendFrame(DataOffset.ToString());
                var rsp= socket.ReceiveFrameString();
                if (rsp != null)
                {
                    if(subscriber==null)
                    {
                       Recvice(rsp);
                    }
                    else
                    {
                        //接受数据
                        subscriber.Connect(rsp);
                    }
                }

            }

        }

        public void Unsubscribe(string topic)
        {
            lst.Add(topic);
        }

        public void Close()
        {
            IsConnected=false;
            subscriber.Close(); 

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
