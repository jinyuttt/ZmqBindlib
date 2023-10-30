using NetMQ;
using NetMQ.Sockets;
using System.Collections.Concurrent;

namespace MQBindlib
{

    /// <summary>
    /// 请求
    /// </summary>
    public  class ZmqRequest
    {
        private static readonly object _monitorLock = new object();
        /// <summary>
        /// 远端地址
        /// </summary>
        public string RemoteAddress { get; set; }=String.Empty;

        /// <summary>
        /// 长链接
        /// </summary>
        private RequestSocket requestSocket = null;

        /// <summary>
        /// 标识
        /// </summary>
        public string? ClientFlage { get; set; } = string.Empty;

        private bool isRun = true;

    
        /// <summary>
        /// 是否有高可用部署
        /// </summary>
        public bool IsCluster { get; set; }= false;

        List<ClusterNode> clusterNodes = new List<ClusterNode>();

      

        ConcurrentDictionary<int, RequestSocket> dic=new ConcurrentDictionary<int, RequestSocket>();


        /// <summary>
        /// 获取高可用数据
        /// </summary>
        private void RequestCluster(RequestSocket  socket=null)
        {

            if (!IsCluster)
            {
                return;
            }
           var task= Task.Factory.StartNew(() =>
            {
                // connect
                try
                {
                    var client = new RequestSocket(RemoteAddress);
                    client.SendMoreFrame(ClientFlage).SendFrame(ConstString.ReqCluster);
                    string msg = client.ReceiveFrameString();
                    List<ClusterNode> lst = Util.JSONDeserializeObject<List<ClusterNode>>(msg);
                    if (lst != null)
                    {
                        clusterNodes = lst;
                        var master = lst.Find(p => p.IsMaster);
                        if (master != null)
                        {
                            if (master.Address != RemoteAddress)
                            {
                                RemoteAddress = master.Address;
                            }
                        }
                    }
                    client.Disconnect(RemoteAddress);
                    client.Close();
                }
                catch (Exception ex) {
                  
                    Console.WriteLine(ex.ToString());
                }
            }).Wait(1000);
            if (!task)
            {
               
                if(clusterNodes!=null)
                {
                   var node= clusterNodes.Where(p=>p.Address!=RemoteAddress).OrderByDescending(p=>p.Id).FirstOrDefault();
                    if(node != null)
                    {
                        RemoteAddress=node.Address;
                    }
                }
                foreach (var kv in dic)
                {
                    kv.Value.Close();
                    kv.Value.Dispose();
                }

                dic.Clear();
                Monitor.TryEnter(_monitorLock, 500);
                if (requestSocket != null)
                {
                    requestSocket.Connect(RemoteAddress);
                }
                Monitor.Exit(_monitorLock);
            }
        
        }

        /// <summary>
        /// 请求
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        public string Request(string msg)
        {
          
            using (var client = new RequestSocket())  // connect
            {
                dic[Thread.CurrentThread.ManagedThreadId] = client;
                RequestCluster(client);
                client.Connect(RemoteAddress);
                client.SendMoreFrame(ClientFlage).SendFrame(msg);
                var ret= client.ReceiveFrameString();
                dic.Remove(Thread.CurrentThread.ManagedThreadId,out var r);  
                return ret;
            }
        }


        /// <summary>
        /// 请求
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        public byte[] Request(byte[] msg)
        {
           
            using (var client = new RequestSocket(RemoteAddress))  // connect
            {
               
                client.SendMoreFrame(ClientFlage).SendFrame(msg);
                return client.ReceiveFrameBytes();

            }
        }

        /// <summary>
        /// 请求
        /// </summary>
        /// <typeparam name="S"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="msg"></param>
        /// <returns></returns>
        public T Request<R,T>(R  msg)
        {
         
            using (var client = new RequestSocket(RemoteAddress))  // connect
            {
              
                var  obj= Util.JSONSerializeObject(msg);
                client.SendMoreFrame(ClientFlage).SendFrame(obj);
                var rsp= client.ReceiveFrameString();
                if(typeof(T) == typeof(string))
                {
                    return (T)Convert.ChangeType(rsp, typeof(T));
                }
                var result= Util.JSONDeserializeObject<T>(rsp);
                return result;
            }

        }

        /// <summary>
        /// 长连接请求
        /// </summary>
        /// <typeparam name="R"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="msg"></param>
        /// <returns></returns>
        public T KeepRequest<R, T>(R msg)
        {
            if (requestSocket == null)
            {
                requestSocket = new RequestSocket(RemoteAddress);
                requestSocket.Options.ReceiveHighWatermark = 0;
                requestSocket.Options.TcpKeepalive = true;
                requestSocket.Options.HeartbeatInterval = new TimeSpan(10000);
                requestSocket.Options.HeartbeatTimeout = new TimeSpan(1000);
                requestSocket.Options.HeartbeatTtl = new TimeSpan(2000);

                Thread cluster = new Thread(p =>
                {
                    while(isRun)
                    {
                        Thread.Sleep(5000);
                        RequestCluster();

                    }
                });
                cluster.IsBackground = true;
                cluster.Name = "requestSocketUp";
                cluster.Start();
              
            }

            var obj = Util.JSONSerializeObject(msg);
            Monitor.TryEnter(_monitorLock, 500);
            requestSocket.SendMoreFrame(ClientFlage).SendFrame(obj);
            var rsp = requestSocket.ReceiveFrameString();
            Monitor.Exit(_monitorLock);
            if (typeof(T) == typeof(string))
            {
                return (T)Convert.ChangeType(rsp, typeof(T));
            }
            var result = Util.JSONDeserializeObject<T>(rsp);

            return result;
        }

        /// <summary>
        /// 关闭长连接
        /// </summary>
        public void Close()
        {
            isRun = false;
            if(requestSocket != null)
            requestSocket.Close();
        }

       
    }
}
