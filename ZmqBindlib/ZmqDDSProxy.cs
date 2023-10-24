using NetMQ;
using NetMQ.Sockets;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace MQBindlib
{

    /// <summary>
    /// 订阅发布代理
    /// </summary>
    public class ZmqDDSProxy
    {
        /// <summary>
        /// 订阅地址
        /// </summary>
        public static string? SubAddress { get; set; }

        /// <summary>
        /// 发布地址
        /// </summary>
        public static  string? PubAddress { get; set; }

        /// <summary>
        /// 是否是集群
        /// </summary>
        public static bool IsCluster { get; set; } = false;

        /// <summary>
        /// 集群名称
        /// </summary>
        public static string ClusterName { get; set; } = "DDSProxy";

        /// <summary>
        /// 集群ID
        /// </summary>
        internal static string ClusterId { get; set; } = string.Empty;

        /// <summary>
        /// 设置为主
        /// </summary>
        public static bool IsMaster { get; set; } = false;


        public static bool  nodemaster=false;

        /// <summary>
        /// 存储数据
        /// </summary>
        public static bool IsStorage { get; set; } = false;

        public static int StorageHours { get; set; }=24;

        private static ConcurrentDictionary<string, KeyNode> dic = new ConcurrentDictionary<string, KeyNode>();

        private static  ConcurrentQueue<InerTopicMessage> topicMessages = new ConcurrentQueue<InerTopicMessage>();

        private static NodeType nodeType = NodeType.XPub;

        private static DBStorage dbStorage = null;
           

        /// <summary>
        /// 启动订阅发布代理
        /// </summary>
        public static void Start()
        {
            Thread thread = new Thread(DDSProxy);
            thread.Name = "ZmqProxy";
            thread.IsBackground = true;
            thread.Start();
            
            MasterProxy();
        }

        private static void DDSProxy()
        {
            var xpubSocket = new XPublisherSocket();
            xpubSocket.Bind(PubAddress);

            var xsubSocket = new XSubscriberSocket();
            xsubSocket.Bind(SubAddress);
            var pub = new PublisherSocket("@inproc://ddsproxy");
            Console.WriteLine(" publish  Intermediary started, and waiting for messages");
            // proxy messages between frontend / backend
            var proxy = new Proxy(xsubSocket, xpubSocket, pub);
            // blocks indefinitely
            proxy.Start();
          

        }
        

        /// <summary>
        /// 集群处理
        /// </summary>
        private static void MasterProxy()
        {
            if(!IsCluster)
            {
                return;
            }
            if(string.IsNullOrEmpty(ClusterId))
            {
                ClusterId = Util.GuidToLongID().ToString(); ;
            }
            Cluster.Remove();
            ZmqBus zmqBus = new ZmqBus();
            Cluster.bus= zmqBus;
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
                        NodeType = NodeType.XPub,
                        IsClusterMaster = IsMaster,
                         SubAddess=SubAddress,

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
                    var lst = Cluster.GetNodes(ClusterName, nodeType);
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
                    var lst = Cluster.GetNodes(ClusterName,nodeType);
                    var master=lst.Find(p => p.IsMaster);
                    if(master != null&&master.Id==ClusterId)
                    {
                        IsMaster=true;
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
                    SubscriberSocket subscriber= new SubscriberSocket();
                    subscriber.Connect(PubAddress);
                    subscriber.Subscribe(ConstString.PubPublisher);
                    while(true)
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
                if (obj != null && obj.NodeType == NodeType.XPub)
                {
                    //只使用同一类型的
                    Cluster.Add(obj);
                    Cluster.Flush(obj.Id);
                }
            }
            if (arg1 == ConstString.UpdateCluster)
            {
                Cluster.UPdateMaster(arg2);
                if(arg2 == ClusterId)
                {
                    IsMaster = true;
                }
            }
            if (arg1 == ConstString.PubCluster)
            {
                //中心交互
                var lst=Util.JSONDeserializeObject<List<string>>(arg2); 
                foreach(var node in lst)
                {
                    Cluster.AddRsp(node);
                }
              
            }
            if(arg1 == ConstString.Storage)
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
        private static KeyNode GetPushSocket(string topic,string id)
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
            int port=  pushSocket.BindRandomPort(tmp);

            keyNode.Socket = pushSocket;
            keyNode.Topic=topic;
            keyNode.Id=id;
            keyNode.Address = pushSocket.Options.LastEndpoint;
          
            return keyNode;
        }

        /// <summary>
        /// 分发消息
        /// </summary>
        private static void Process()
        {
            Thread thread = new Thread(p => {

                while (true)
                {
                    if(topicMessages.TryDequeue(out var msg))
                    {
                     
                      
                        foreach (var kv in dic)
                        {
                            if(kv.Key.StartsWith(msg.Topic))
                            {
                                kv.Value.Socket.SendMoreFrame(msg.Topic).SendFrame(msg.Message);
                               
                            }
                        }
                    }
                    if(topicMessages.IsEmpty)
                    {
                        Thread.Sleep(1000);
                    }    
                }
            });
            thread.Name = "push";
            thread.Start();
        }

        /// <summary>
        /// 启动分组拉取（类似kafka)
        /// </summary>
        public static void StartProxy()
        {
            if(IsStorage)
            {
                dbStorage = new DBStorage();
            }
            nodeType = NodeType.Poll;
            ReqProxy();
            Process();
            MasterProxy();

        }
        private static void ReqProxy()
        {
            Thread thread = new Thread(p =>
            {
                //中心代理
                var xsubSocket = new XSubscriberSocket();
                xsubSocket.Bind(SubAddress);
                var xpubSocket=new XPublisherSocket();
                xpubSocket.Bind("inproc://ddsproxy");
                Console.WriteLine(" publish  Intermediary started, and waiting for messages");
                var proxy = new Proxy(xsubSocket, xpubSocket);
              
                proxy.Start();
               

            });
            thread.Name = "proxy";
            thread.Start();

            Thread rspThread = new Thread(p =>
            {
                //接受Pull订阅信息
                var rsp = new ResponseSocket();
                rsp.Bind(PubAddress);
                while (true)
                {
                    var topic = rsp.ReceiveFrameString();
                    var id = rsp.ReceiveFrameString();
                    var offset = rsp.ReceiveFrameString();
                    var sock = dic.GetOrAdd(topic + id, GetPushSocket(topic, id));
                    rsp.SendFrame(sock.Address);
                    DataModel model= Enum.Parse<DataModel>(offset);
                    if (model == DataModel.Earliest)
                    {
                        Task.Factory.StartNew(() =>
                        {
                           var off=  (DateTime.Now.Ticks / 10000) - StorageHours * 60 * 60 * 1000;
                            dbStorage.Remove(topic,off);
                            var lst = dbStorage.GetMessage(topic);
                            foreach (var item in lst)
                            {
                                sock.Socket.SendMoreFrame(item.Topic).SendFrame(item.Message);
                            }
                        });
                        
                    }
                }

            });
            rspThread.Name = "ResponseSocket";
            rspThread.IsBackground = true;
            rspThread.Start();

            Thread sub = new Thread(p =>
            {
                //接受发布端数据
                Thread.Sleep(2000);
                var subscriber = new SubscriberSocket();
                subscriber.Connect("inproc://ddsproxy");
                subscriber.SubscribeToAnyTopic();
                while(true)
                {
                   var topic= subscriber.ReceiveFrameString();
                   var data= subscriber.ReceiveFrameString();
                    if(ConstString.PubPublisher==topic)
                    {
                        Cluster.AddRsp(data);
                        continue;
                    }
                  
                    topicMessages.Enqueue(new InerTopicMessage() { Topic=topic,Message = data });
                    if (!nodemaster)
                    {
                        var lst = Cluster.GetNodes(ClusterName, nodeType);
                        var master = lst.Find(p => p.IsMaster);
                        if (master != null && master.Id == ClusterId)
                        {
                            nodemaster = true;
                        }
                    }
                    if (nodemaster)
                    {
                        Cluster.bus.Publish(ConstString.Storage, new InerTopicMessage() { Topic = topic, Message = data });
                     
                    }
                    if(IsStorage)
                    {
                        dbStorage.Add(topic, data);
                    }
                }

            });
            sub.Name = "sub";
            sub.Start();
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
        public string Topic { get; set;}

        /// <summary>
        /// ID
        /// </summary>
        public string Id { get; set;}  

        /// <summary>
        /// socket
        /// </summary>
        public PushSocket Socket  { get; set;}


        public string Address { get; set;}
    }
}
