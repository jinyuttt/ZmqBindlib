using NetMQ;
using NetMQ.Sockets;

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

      

      

       

      

        private static ManualResetEventSlim eventSlim = new ManualResetEventSlim();
        private static ManualResetEventSlim  resetEventSlim = new ManualResetEventSlim();
        private static bool isSucess = false;
        private static bool isPullSucess = false;



        /// <summary>
        /// 启动订阅发布代理
        /// </summary>
        public static bool Start()
        {
            Thread thread = new Thread(DDSProxy);
            thread.Name = "ZmqProxy";
            thread.IsBackground = true;
            thread.Start();
            eventSlim.Wait();
            if (isSucess)
            {
                MasterProxy();
            }
            return isSucess;
        }

        private static void DDSProxy()
        {
            try
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
                isSucess = true;
                eventSlim.Set();
                proxy.Start();
            }
            catch(NetMQException e) {
                Logger.Singleton.Error("启动失败", e);
            }
            eventSlim.Set();

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
                    var lst = Cluster.GetNodes(ClusterName, NodeType.XPub);
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
                    var lst = Cluster.GetNodes(ClusterName,NodeType.XPub);
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
            
        }

     
    }
 
  
}
