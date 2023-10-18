using NetMQ;
using NetMQ.Sockets;
using System.Collections.Concurrent;
using System.Text;


namespace MQBindlib
{
    /// <summary>
    /// 处理服务
    /// </summary>
    public class EhoServer
    {
        const string resp = "ehoserver";

        //收到的数据
        readonly BlockingCollection<RspSocket<string>> queue = new();

        /// <summary>
        /// 次优先，字符串
        /// </summary>
        public event EventHandler<RspSocket<string>> StringReceived;

        /// <summary>
        /// 最优先，返回byte[]
        /// </summary>
        public event EventHandler<RspSocket<byte[]>> ByteReceived;


        /// <summary>
        /// 正在使用的
        /// </summary>
        private readonly ConcurrentDictionary<string, ResponseSocket> dicSocket= new ConcurrentDictionary<string, ResponseSocket>();



       /// <summary>
       /// 阻塞释放
       /// </summary>
        private readonly ConcurrentDictionary<string, ManualResetEventSlim> dicManualResetEvent = new ConcurrentDictionary<string, ManualResetEventSlim>();


        /// <summary>
        /// 空闲的
        /// </summary>
        private readonly List<ResponseSocket> lstSockets = new List<ResponseSocket>();

        /// <summary>
        /// 缓存
        /// </summary>

        private readonly ConcurrentBag<ManualResetEventSlim>  eventSlims=new ConcurrentBag<ManualResetEventSlim>();


        private long curTikcs= DateTime.Now.Ticks;

        private readonly Random random = new Random();

        /// <summary>
        /// 记录启动个数
        /// </summary>
      private  int rspNum = 0;

        
       

        /// <summary>
        /// 不用设置，进程内通讯
        /// </summary>
        public string? DealerAddress { get; set; } = "inproc://ehoserver";


        /// <summary>
        /// 服务地址
        /// </summary>
        public string? RouterAddress { get; set; }= "tcp://127.0.0.1:5560";

        /// <summary>
        /// 最大线程数,默认100
        /// </summary>
        public int MaxProcessThreadNum { get; set; } = 100;

        /// <summary>
        /// 是否所有请求都没有回复
        /// </summary>
        public bool IsEmptyReturn { get; set; }= false;

        /// <summary>
        /// 是否是集群
        /// </summary>
        public  bool IsCluster { get; set; } = false;

        /// <summary>
        /// 集群名称
        /// </summary>
        public string ClusterName { get; set; } = "ehoserver";


        /// <summary>
        /// 集群ID
        /// </summary>
        public string ClusterId { get; set; } = string.Empty;


        public bool IsClusterMaster {  get; set; } = false;

      private string serverid=string.Empty;

        private bool IsRun = true;

        /// <summary>
        /// 启动
        /// </summary>
        public  void Start()
        {
            
            REPProxy();
            Thread.Sleep(1000);//让代理线启动，进程内通讯先要绑定地址
            Check();
            Flush();
            RspCluster();
        }

        /// <summary>
        /// 启动代理，代理是阻塞的
        /// </summary>
        private  void REPProxy()
        {
            ZmqProxy.DealerAddress = DealerAddress;
            ZmqProxy.RouterAddress = RouterAddress;
            serverid=Util.GuidToLongID().ToString();
            ZmqProxy.Start(serverid);
            Logger.Singleton.Info(string.Format("代理启动：RouterAddress:{0},DealerAddress:{1}", RouterAddress, DealerAddress));
        }
        
        private void RspCluster()
        {
            if(!IsCluster)
            {
                return;
            }
            if (ClusterId == string.Empty)
            {
                ClusterId = Util.GuidToLongID().ToString();
            }
            Cluster.Remove();
            ZmqBus zmqBus = new ZmqBus();
            Cluster.bus = zmqBus;
            ClusterNode node = new ClusterNode()
            {
                Name = ClusterName,
                Id = ClusterId,
                Address = RouterAddress,
                NodeType = NodeType.Request,
                 IsClusterMaster = IsClusterMaster,
            };
          
            zmqBus.Subscribe(ConstString.ReqCluster);
            zmqBus.Subscribe(ConstString.UpdateCluster);
            zmqBus.StringReceived += ZmqBus_StringReceived;
           
            Thread nodeTh = new Thread(p =>
            {
                ZmqBus tmp= new ZmqBus();
                tmp.Publish(ConstString.ReqCluster, node);
                while (IsRun)
                {
                    Thread.Sleep(5000);
                    tmp.Publish(ConstString.ReqCluster, node);
                }
               //退出后注销
             zmqBus.StringReceived -= ZmqBus_StringReceived;
            }
            );
            nodeTh.Start();
        }

        private void ZmqBus_StringReceived(string arg1, string arg2)
        {
            if (arg1 == ConstString.ReqCluster)
            {
                var obj = Util.JSONDeserializeObject<ClusterNode>(arg2);
                if (obj != null && obj.NodeType == NodeType.Request)
                {
                    //只使用同一类型的
                    Cluster.Add(obj);
                    Cluster.Flush(obj.Id);
                }
            }
            if(arg1 == ConstString.UpdateCluster)
            {
                Cluster.UPdateMaster(ClusterId);
            }
          
        }

        /// <summary>
        /// 移除Rsp
        /// </summary>
        private void Flush()
        {
                Thread thread = new Thread(Remove);
                thread.Name = "removeRsp";
                thread.IsBackground = true;
                thread.Start();
        }

        /// <summary>
        /// 检查使用情况
        /// </summary>
        private void  Check()
        {
            curTikcs = DateTime.Now.Ticks;
            if (lstSockets.Count < 10)
            {
                if (Interlocked.Increment(ref rspNum) < MaxProcessThreadNum)
                {
                    ThreadPool.QueueUserWorkItem(CreateRsp);
                }
            }
           
            
        }

        /// <summary>
        /// 移除空闲
        /// </summary>
        private void Remove()
        {
            while (IsRun)
            {
                Thread.Sleep(5000);
                long ticks = DateTime.Now.Ticks - curTikcs;
                long per = ticks / 10000000 + 1;
                //使用频率
                if (dicSocket.Count / per < 10)
                {
                    Logger.Singleton.Debug("清理空闲处理线程");
                    //空闲保留10
                    while (lstSockets.Count > 10)
                    {
                        var rsp = lstSockets[0];
                        lstSockets.RemoveAt(0);
                        if (rsp != null && !rsp.IsDisposed)
                        {
                            rsp.Disconnect(DealerAddress);
                            rsp.Close();
                            rsp.Dispose();

                        }
                    }
                    //检查缓存
                    while(eventSlims.Count>dicSocket.Count+lstSockets.Count+5)
                    {
                        eventSlims.TryTake(out var rsp);
                    }
                }
                Logger.Singleton.Info(string.Format("处理线程：{0}",rspNum));
            }
        }

        /// <summary>
        /// 生成后端处理
        /// </summary>
        private  void CreateRsp(object? oj)
        {
           var server = new ResponseSocket();
            server.Options.Linger = new TimeSpan(10000);
            server.Connect(DealerAddress);
            while (IsRun)
            {
                try
                {
                    string client = server.ReceiveFrameString();
                    string key = random.Next(1, int.MaxValue).ToString();
                    dicSocket[key] = server;//存储到使用
                    lstSockets.Remove(server);//空闲中移除
                    Check();//启动预留
                   
                    if (ByteReceived != null)
                    {
                        var bytes = server.ReceiveFrameBytes();
                        if(bytes != null)
                        {
                           var data=  Encoding.UTF8.GetString(bytes);

                            if (RspCluster(server,data))
                            {
                                continue;
                            }
                        }
                        var rsp = new RspSocket<byte[]> { Message = bytes, responseSocket = server, key = key, ehoServer = this, Client=client };

                        ByteReceived(this, rsp);

                    }
                    else if (StringReceived != null)
                    {
                        var msg = server.ReceiveFrameString();
                        if (RspCluster(server, msg))
                        {
                            continue;
                        }
                        var rsp = new RspSocket<string> { Message = msg, responseSocket = server, key = key, ehoServer = this, Client = client };

                        StringReceived(this, rsp);
                    }
                    else
                    {
                        if (eventSlims.IsEmpty)
                        {
                            eventSlims.Add(new ManualResetEventSlim(false));
                        }
                        if (eventSlims.TryTake(out var resetEventSlim))
                        {
                            resetEventSlim.Reset();
                            var msg = server.ReceiveFrameString();
                            if (RspCluster(server, msg))
                            {
                                continue;
                            }
                            var rsp = new RspSocket<string>() { responseSocket = server, Message = msg, key = key, Client = client };
                            dicManualResetEvent[key] = resetEventSlim;
                            queue.Add(rsp);
                            if(!IsEmptyReturn)
                            {
                                resetEventSlim.Wait();//等待
                            }
                           
                        }
                    }
                    if (IsEmptyReturn)
                    {
                        server.SendFrame(resp);
                        this.Response(key);
                    }
                }
                catch (System.Net.Sockets.SocketException ex)
                {
                    if (ex.ErrorCode == 10054)
                    {
                        break;
                    }
                }
                catch (ObjectDisposedException ex)
                {
                    if (server.IsDisposed)
                    {
                        break;
                    }
                }
            }
            Interlocked.Decrement(ref rspNum);//记录退出

        }
        

        private bool RspCluster(ResponseSocket rsp,string data)
        {
            if (IsCluster)
            {
                if (ConstString.ReqCluster == data)
                {
                    var r = Util.JSONSerializeObject(Cluster.GetNodes(ClusterName,NodeType.Request));
                    rsp.SendFrame(r);
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// 获取接收的数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public RspSocket<T> GetMsg<T>()
        {
            RspSocket<string> result = queue.Take();
            T obj= Util.JSONDeserializeObject<T>(result.Message);
            return new RspSocket<T>() { Message = obj,responseSocket=result.responseSocket, ehoServer = this, key=result.key, Client=result.Client };
        }


        public void Close()
        {
            ZmqProxy.Close(serverid);
            IsRun = false;
            dicManualResetEvent.Clear();
            foreach(var kv in dicSocket)
            {
                kv.Value.Close();

            }
            lstSockets.ForEach(x => x.Close());
            eventSlims.Clear();
        }
        /// <summary>
        /// 回复数据
        /// </summary>
        /// <param name="key"></param>
        internal void Response(string key)
        {
            if (dicSocket.TryRemove(key, out var rsp))
            {
                if (!rsp.IsDisposed)
                {
                    lstSockets.Add(rsp);
                }
               
            }
            if(dicManualResetEvent.TryRemove(key,out var resetEventSlim))
            {
                resetEventSlim.Set();
                eventSlims.Add(resetEventSlim);//继续使用

            }
        }
   
    
    }
}
