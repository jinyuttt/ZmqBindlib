using NetMQ;
using NetMQ.Sockets;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace MQBindlib
{

    /// <summary>
    /// 订阅发布代理
    /// </summary>
    public class ZmqPullProxy
    {
        /// <summary>
        /// 订阅地址
        /// </summary>
        public static string? SubAddress { get; set; }

        /// <summary>
        /// 发布地址
        /// </summary>
        public static string? PubAddress { get; set; }

        /// <summary>
        /// 是否是集群
        /// </summary>
        public static bool IsCluster { get; set; } = false;

        /// <summary>
        /// 集群名称
        /// </summary>
        public static string ClusterName { get; set; } = "PullProxy";

        /// <summary>
        /// 集群ID
        /// </summary>
        internal static string ClusterId { get; set; } = string.Empty;

        /// <summary>
        /// 设置为主
        /// </summary>
        public static bool IsMaster { get; set; } = false;

        
        /// <summary>
        /// 存储数据
        /// </summary>
        public static bool IsStorage { get; set; } = false;

        /// <summary>
        /// 存储时间，小时
        /// </summary>
        public static int StorageHours { get; set; } = 24;


        public static int MaxThreadNum { get; set; } = 10;

        /// <summary>
        /// 超过量启动分发
        /// </summary>
        public static int MaxPackNum { get; set; } = 10000;

        /// <summary>
        /// 线程数
        /// </summary>
        public static int processNum = 0;


        private const string inprocAddress = "inproc://pullproxy";

        public static bool nodemaster = false;


        private static readonly ConcurrentDictionary<string, KeyNode> dic = new ConcurrentDictionary<string, KeyNode>();

        private static readonly ConcurrentQueue<InerTopicMessage> topicMessages = new ConcurrentQueue<InerTopicMessage>();

        private static DBStorage dbStorage = null;

        
        private static ManualResetEventSlim resetEventSlim = new ManualResetEventSlim();
      
        private static bool isPullSucess = false;



        /// <summary>
        /// 启动分组拉取（类似kafka)
        /// </summary>
        public static bool Start()
        {
            if (IsStorage)
            {
                dbStorage = new DBStorage();
            }
          
            ReceivePublishProxy();
            if (isPullSucess)
            {
                Process();
                MasterProxy();
            }
            return isPullSucess;
        }

        /// <summary>
        /// 接收发布的数据
        /// </summary>
        private static void ReceivePublishProxy()
        {
            Thread thread = new Thread(() =>
            {
                //中心代理
                try
                {
                    var xsubSocket = new XSubscriberSocket();
                    xsubSocket.Options.TcpKeepalive = true;
                    xsubSocket.Options.TcpKeepaliveInterval = TimeSpan.FromSeconds(5);
                    xsubSocket.Options.TcpKeepaliveIdle = TimeSpan.FromSeconds(10);
                    xsubSocket.Options.HeartbeatTtl = TimeSpan.FromSeconds(5);
                    xsubSocket.Options.HeartbeatTimeout = TimeSpan.FromSeconds(2);
                    xsubSocket.Options.HeartbeatInterval = TimeSpan.FromSeconds(10);
                    xsubSocket.Bind(SubAddress);

                    var xpubSocket = new XPublisherSocket();

                    xpubSocket.Options.TcpKeepalive = true;
                    xpubSocket.Options.TcpKeepaliveInterval = TimeSpan.FromSeconds(5);
                    xpubSocket.Options.TcpKeepaliveIdle = TimeSpan.FromSeconds(10);
                    xpubSocket.Options.HeartbeatTtl = TimeSpan.FromSeconds(5);
                    xpubSocket.Options.HeartbeatTimeout = TimeSpan.FromSeconds(2);
                    xpubSocket.Options.HeartbeatInterval = TimeSpan.FromSeconds(10);
                    xpubSocket.Bind(inprocAddress);
                    Logger.Singleton.Info("start proxy");
                    var proxy = new Proxy(xsubSocket, xpubSocket);
                  
                    isPullSucess = true;//设置成功
                    resetEventSlim.Set();
                    proxy.Start();

                }
                catch (Exception ex)
                {
                    Logger.Singleton.Error("启动失败", ex);
                }
                resetEventSlim.Set();

            });

            thread.Name = "pullproxy";
            thread.Start();
            resetEventSlim.Wait();
            if (!isPullSucess)
            {
                return;
            }
            Thread rspThread = new Thread(()=>
            {
                //接受Pull订阅信息
                var rsp = new ResponseSocket();
                rsp.Bind(PubAddress);
                while (true)
                {
                    var topic = rsp.ReceiveFrameString();//订阅的主题
                    var id = rsp.ReceiveFrameString();//消费分组ID
                    var offset = rsp.ReceiveFrameString();//数据需求
                    if (topic == ConstString.HeartbeatTopic)
                    {
                        if (string.IsNullOrEmpty(ClusterId))
                        {
                            ClusterId = Util.GuidToLongID().ToString();
                        }
                        rsp.SendFrame(ClusterId);
                        continue;
                    }
                    var sock = dic.GetOrAdd(topic + id, GetPushSocket(topic, id));//分配数据
                    rsp.SendFrame(sock.Address);//回复地址
                    Logger.Singleton.Info($"订阅主题:{topic},组ID:{id}");
                    DataModel model = Enum.Parse<DataModel>(offset);
                    if (model == DataModel.Earliest)
                    {
                        Task.Factory.StartNew(() =>
                        {
                            var off = (DateTime.Now.Ticks / 10000) - StorageHours * 60 * 60 * 1000;//按秒计算
                            dbStorage.Remove(topic, off);//先触发移除数据
                            var lst = dbStorage.GetMessage(topic);//获取数据
                            foreach (var item in lst)
                            {
                                //所有数据发送一次
                                sock.Socket.SendMoreFrame(item.Topic).SendFrame(item.Message);
                            }
                        });

                    }
                }

            });
            rspThread.Name = "ResponseSocket";
            rspThread.IsBackground = true;
            rspThread.Start();

            Thread sub = new Thread(() =>
            {
                //接受发布端数据
                Thread.Sleep(2000);
                var subscriber = new SubscriberSocket();
                subscriber.Connect(inprocAddress);
                subscriber.SubscribeToAnyTopic();//接收发布端所有数据
                while (true)
                {
                    var topic = subscriber.ReceiveFrameString();
                    var data = subscriber.ReceiveFrameString();
                    if (ConstString.PubPublisher == topic)
                    {
                        //高可用时发布方发布自己的获取中心节点的地址
                        Cluster.AddRsp(data);
                        continue;
                    }

                    //数据获取
                    topicMessages.Enqueue(new InerTopicMessage() { Topic = topic, Message = data });
                    if (IsCluster)
                    {
                        if (!nodemaster)
                        {
                            var lst = Cluster.GetNodes(ClusterName, NodeType.Poll);
                            var master = lst.Find(p => p.IsMaster);
                            if (master != null && master.Id == ClusterId)
                            {
                                //当前是否是主节点
                                nodemaster = true;
                            }
                        }
                        if (nodemaster)
                        {
                            //节点见互传数据
                            Cluster.bus.Publish(ConstString.Storage, new InerTopicMessage() { Topic = topic, Message = data });

                        }
                    }
                    if (IsStorage)
                    {
                        //存储数据
                        dbStorage.Add(topic, data);
                    }
                }

            });
            sub.Name = "sub";
            sub.Start();
        }
        /// <summary>
        /// 高可用处理
        /// </summary>
        private static void MasterProxy()
        {
            if (!IsCluster)
            {
                return;
            }
            if (string.IsNullOrEmpty(ClusterId))
            {
                ClusterId = Util.GuidToLongID().ToString(); ;
            }
            Cluster.Remove();
            ZmqBus zmqBus = new ZmqBus();
            Cluster.bus = zmqBus;
            zmqBus.Subscribe(ConstString.RspCluster);
            zmqBus.Subscribe(ConstString.UpdateCluster);
            zmqBus.Subscribe(ConstString.PubCluster);
            zmqBus.Subscribe(ConstString.Storage);
            //中心之间
            zmqBus.StringReceived += ZmqBus_StringReceived;
            Thread monitor = new Thread(p =>
            {
                ZmqBus bus = new ZmqBus();
                while (true)
                {
                    Thread.Sleep(5000);
                    ClusterNode clusterNode = new ClusterNode()
                    {
                        Address = PubAddress,
                        Name = ClusterName,
                        Id = ClusterId,
                        NodeType = NodeType.Poll,
                        IsClusterMaster = IsMaster,
                        SubAddess = SubAddress,

                    };
                    //中心节点刷新
                    bus.Publish(ConstString.RspCluster, clusterNode);

                    var rsp = Cluster.GetRsp();
                    //中心刷新发布方消息
                    bus.Publish(ConstString.PubCluster, rsp);
                }
            });
            monitor.Start();

            Thread timer = new Thread(p =>
            {
                //中心节点定时向订阅方发布中心节点（通过发布方式），不管是pull还是sub
                PublisherSocket publisherSocket = new PublisherSocket();
                publisherSocket.Connect(SubAddress);
                while (true)
                {
                    Thread.Sleep(2000);
                    var lst = Cluster.GetNodes(ClusterName, NodeType.Poll);
                    var msg = Util.JSONSerializeObject(lst);
                    publisherSocket.SendMoreFrame(ConstString.ReqCluster).SendFrame(msg);

                }

            });
            timer.IsBackground = true;
            timer.Name = "toSubscriber";
            timer.Start();

            //

            Thread pub = new Thread(p =>
            {
                //中心节点定时向发布方发布节点

                while (true)
                {
                    Thread.Sleep(2000);
                    //发布方启动自己的节点
                    var lst = Cluster.GetNodes(ClusterName, NodeType.Poll);
                    var master = lst.Find(p => p.IsMaster);
                    if (master != null && master.Id == ClusterId)
                    {
                        IsMaster = true;
                        //作用不大，控制Masters刷新
                        var msg = Util.JSONSerializeObject(lst);
                        var rsps = Cluster.GetRsp();
                        foreach (var node in rsps)
                        {
                            Task.Factory.StartNew(() => {
                                using (RequestSocket request = new RequestSocket(node))
                                {
                                    try
                                    {
                                        request.SendFrame(msg);
                                        request.ReceiveFrameString();
                                    }
                                    catch (Exception e)
                                    {
                                        Logger.Singleton.Error(e.Message);
                                    }
                                }
                            }
                            ).Wait(1000);

                        }
                    }


                }

            });
            pub.IsBackground = true;
            pub.Name = "ClusterToPub";
            pub.Start();
            Thread recpub = new Thread(p =>
            {
                //中心节点接受发布方消息
                SubscriberSocket subscriber = new SubscriberSocket();
                subscriber.Connect(PubAddress);
                subscriber.Subscribe(ConstString.PubPublisher);
                while (true)
                {
                    var topic = subscriber.ReceiveFrameString();
                    var msg = subscriber.ReceiveFrameString();
                    Cluster.AddRsp(msg);
                }
            });
            recpub.IsBackground = true;
            recpub.Name = "recviceRsp";
            recpub.Start();
        }

        private static void ZmqBus_StringReceived(string arg1, string arg2)
        {
            if (arg1 == ConstString.RspCluster)
            {
                var obj = Util.JSONDeserializeObject<ClusterNode>(arg2);
                if (obj != null && obj.NodeType == NodeType.Poll)
                {
                    //只使用同一类型的
                    Cluster.Add(obj);
                    Cluster.Flush(obj.Id);
                }
            }
            if (arg1 == ConstString.UpdateCluster)
            {
                Cluster.UPdateMaster(arg2);
                if (arg2 == ClusterId)
                {
                    IsMaster = true;
                }
            }
            if (arg1 == ConstString.PubCluster)
            {
                //中心交互
                var lst = Util.JSONDeserializeObject<List<string>>(arg2);
                foreach (var node in lst)
                {
                    Cluster.AddRsp(node);
                }

            }
            if (arg1 == ConstString.Storage)
            {
                var obj = Util.JSONDeserializeObject<InerTopicMessage>(arg2);
                dbStorage.Add(obj.Topic, obj.Message);
            }
        }

        /// <summary>
        /// 创建消息Push
        /// </summary>
        /// <param name="topic">主题</param>
        /// <param name="id">分组ID</param>
        /// <returns></returns>
        private static KeyNode GetPushSocket(string topic, string id)
        {
            KeyNode keyNode = new KeyNode();
            string tmp = "tcp://*";
            string pattrn = @"(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])";
            var mat = Regex.Match(SubAddress, pattrn);
            if (mat.Success)
            {
                //根据订阅IP设置具体IP
                tmp = "tcp://" + mat.Value;
            }
            var pushSocket = new PushSocket();
            pushSocket.Options.TcpKeepalive = true;
            pushSocket.Options.TcpKeepaliveInterval = TimeSpan.FromSeconds(5);
            pushSocket.Options.TcpKeepaliveIdle = TimeSpan.FromSeconds(10);
            pushSocket.Options.HeartbeatTtl = TimeSpan.FromSeconds(5);
            pushSocket.Options.HeartbeatTimeout = TimeSpan.FromSeconds(2);
            pushSocket.Options.HeartbeatInterval = TimeSpan.FromSeconds(10);
            pushSocket.Options.SendHighWatermark = 0;
          
            int port = pushSocket.BindRandomPort(tmp);

            keyNode.Socket = pushSocket;
            keyNode.Topic = topic;
            keyNode.Id = id;
            keyNode.Address = pushSocket.Options.LastEndpoint;

            return keyNode;
        }


        private static void Pack(Object obj)
        {
            while (true)
            {
                if (topicMessages.TryDequeue(out var msg))
                {
                    foreach (var kv in dic)
                    {
                        if (kv.Key.StartsWith(msg.Topic))
                        {
                          
                            kv.Value.Socket.TrySendFrame(TimeSpan.FromSeconds(1), msg.Topic, true);
                            kv.Value.Socket.TrySendFrame(TimeSpan.FromSeconds(1), msg.Message, false);

                        }
                    }
                }
                if (topicMessages.IsEmpty)
                {
                    break;
                }
            }
            Interlocked.Decrement(ref processNum);
            Logger.Singleton.Info($"分发线程：{processNum}");
        }

        /// <summary>
        /// 分发消息
        /// </summary>
        private static void Process()
        {
            Thread thread = new Thread(p => {

                while (true)
                {
                    if(topicMessages.Count>MaxPackNum&&processNum<MaxThreadNum)
                    {
                        Interlocked.Increment(ref processNum);
                        ThreadPool.QueueUserWorkItem(Pack);
                        Logger.Singleton.Info($"分发线程：{processNum}");
                    }
                    if (topicMessages.TryDequeue(out var msg))
                    {


                        foreach (var kv in dic)
                        {
                            if (kv.Key.StartsWith(msg.Topic))
                            {
                                //  kv.Value.Socket.SendMoreFrame(msg.Topic).SendFrame(msg.Message);
                                kv.Value.Socket.TrySendFrame(TimeSpan.FromSeconds(1), msg.Topic,true);
                                kv.Value.Socket.TrySendFrame(TimeSpan.FromSeconds(1), msg.Message, false);

                            }
                        }
                    }
                    if (topicMessages.IsEmpty)
                    {
                        Thread.Sleep(1000);
                    }
                }
            });
            thread.IsBackground = true;
            thread.Name = "push";
            thread.Start();
        }

       
    
    }
    /// <summary>
    /// 
    /// </summary>
    public class KeyNode
    {
        /// <summary>
        ///主题
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        /// ID
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// socket
        /// </summary>
        public PushSocket Socket { get; set; }


        public string Address { get; set; }
    }
}
