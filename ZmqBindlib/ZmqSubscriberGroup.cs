using NetMQ;
using NetMQ.Sockets;
using System.Collections.Concurrent;

namespace MQBindlib
{
    /// <summary>
    /// kafka模式订阅
    /// </summary>
    public class ZmqSubscriberGroup
    {
        PullSocket  subscriber = null;
        readonly BlockingCollection<InerTopicMessage> queue = new BlockingCollection<InerTopicMessage>();

       

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


        /// <summary>
        /// 分组标识
        /// </summary>
        public string Indenty { get; set; } = "mqbindlib";

        /// <summary>
        /// 接收数据超时刷新，单位：秒，默认：30
        /// </summary>
        public int HeartbeatTtl { get; set; } = 30;

        /// <summary>
        /// 获取数据
        /// </summary>
        public DataModel DataOffset=DataModel.Latest;

        /// <summary>
        /// 有高可用部署
        /// </summary>
        public bool IsDDS { get; set; }

        private bool IsConnected = true;

        private readonly List<string> topics = new List<string>();
         List<ClusterNode> lstNode = new List<ClusterNode>();//高可用节点

        private readonly ConcurrentDictionary<string,string> dicRsp=new ConcurrentDictionary<string, string>();

        DateTime fulshTime = DateTime.Now;//中心节点刷新时间

        DateTime dataFlush=DateTime.Now;//接收数据

        private string serverid =string.Empty;//中心标识

        private readonly TimeSpan m_deadNodeTimeout = TimeSpan.FromSeconds(10);//中心刷新超时时间

      /// <summary>
      /// 更新数据和中心(高可用有效)
      /// </summary>
        private void Update()
        {
            Thread up = new Thread(() =>
            {
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
            up.IsBackground=true;
            up.Start();
        }


        /// <summary>
        /// 接受数据
        /// </summary>
        /// <param name="address"></param>
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
                subscriber.Options.TcpKeepalive = true;
                subscriber.Options.TcpKeepaliveInterval=TimeSpan.FromSeconds(5);
                subscriber.Options.TcpKeepaliveIdle=TimeSpan.FromSeconds(10);
                subscriber.Options.HeartbeatTtl = TimeSpan.FromSeconds(5);
                subscriber.Options.HeartbeatTimeout = TimeSpan.FromSeconds(2);
                subscriber.Options.HeartbeatInterval = TimeSpan.FromSeconds(10);
                subscriber.Connect(address);
                Reset(subscriber);
                Thread rec = new Thread(() =>
                {
                    while (IsConnected)
                    {

                        try
                        {
                            var topic = subscriber.ReceiveFrameString();
                            dataFlush=DateTime.Now;
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
        /// 中心更换异常时，定时重新订阅
        /// </summary>
        /// <param name="pull"></param>
        private void Reset(PullSocket pull)
        {
            Task.Factory.StartNew(() =>
            {
                if (string.IsNullOrEmpty(serverid))
                {
                    //初始化
                    using (var socket = new RequestSocket(Address))
                    {
                        socket.SendMoreFrame(ConstString.HeartbeatTopic).SendMoreFrame(Indenty).SendFrame(DataOffset.ToString());
                        var rsp = socket.ReceiveFrameString();
                        serverid = rsp;
                    }
                   
                }
                Thread.Sleep(5000);
                if (DateTime.Now - dataFlush < TimeSpan.FromSeconds(HeartbeatTtl))
                {
                    Reset(pull);
                    return;
                }
                using (var socket = new RequestSocket(Address))
                {

                    socket.SendMoreFrame(ConstString.HeartbeatTopic).SendMoreFrame(Indenty).SendFrame(DataOffset.ToString());
                    var rsp = socket.ReceiveFrameString();
                    if(rsp==serverid)
                    {
                        //没有更换
                        Reset(pull);
                        return;
                    }
                    serverid = rsp;
                }

                //主题重新订阅
                var curTopic = topics.ToHashSet();
                topics.Clear();
                foreach (var topic in curTopic)
                {
                    if(pull.IsDisposed) { continue; }
                    //已经更新了地址
                    Subscribe(topic);//重新订阅
                }
                Reset(pull);
            });
           
        }

        /// <summary>
        /// 订阅主题
        /// </summary>
        /// <param name="topic"></param>
        public void Subscribe(string topic)
        {
           
                if (!topics.Contains(topic))
                {
                    topics.Add(topic);//记录主题
                }
                else
                {
                    return;
                }
            
           
            using(var socket = new RequestSocket(Address))
            {
           
                socket.SendMoreFrame(topic).SendMoreFrame(Indenty).SendFrame(DataOffset.ToString());
                var rsp= socket.ReceiveFrameString();
                if (rsp != null)
                {
                    dicRsp[topic] = rsp;
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

        /// <summary>
        /// 取消订阅
        /// </summary>
        /// <param name="topic"></param>
        public void Unsubscribe(string topic)
        {
          
            topics.Remove(topic);
            if(subscriber!=null)
            {
                if(dicRsp.TryGetValue(topic, out var addr))
                {
                    Task.Factory.StartNew(() => {
                        try
                        {
                            if (!subscriber.IsDisposed)
                            {
                                subscriber.Disconnect(addr);
                            }
                        }

                        catch (Exception e)
                        {
                            Logger.Singleton.Error("取消订阅", e);
                        }
                    });
                }
                
            }
        }

        public void Close()
        {
            IsConnected=false;
            foreach(var topic in topics.ToHashSet())
            {
                Unsubscribe(topic);
            }
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
