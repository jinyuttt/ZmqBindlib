using NetMQ;
using NetMQ.Sockets;
using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;

namespace ZmqBindlib
{
    /// <summary>
    /// 处理服务
    /// </summary>
    public class EhoServer
    {

        readonly BlockingCollection<RspSocket<string>> queue = new();

        /// <summary>
        /// 次优先，字符串
        /// </summary>
        public event EventHandler<RspSocket<string>> StringReceived;


        /// <summary>
        /// 正在使用的
        /// </summary>
        private readonly ConcurrentDictionary<string, ResponseSocket> dicSocket= new ConcurrentDictionary<string, ResponseSocket>();

        private readonly ConcurrentDictionary<string, ManualResetEventSlim> dicManualResetEvent = new ConcurrentDictionary<string, ManualResetEventSlim>();


        /// <summary>
        /// 空闲的
        /// </summary>
        private readonly List<ResponseSocket> lstSockets = new List<ResponseSocket>();

       
        long curTikcs= System.DateTime.Now.Ticks;

        readonly Random random = new Random();

        /// <summary>
        /// 记录启动个数
        /// </summary>
        int rspNum = 0;

        
        /// <summary>
        /// 最优先，返回byte[]
        /// </summary>
        public event EventHandler<RspSocket<byte[]>> ByteReceived;

        /// <summary>
        /// 分发地址Dealer
        /// </summary>
        public string? DealerAddress { get; set; } = "inproc://ehoserver";


        /// <summary>
        /// RouterAddress
        /// </summary>
        public string? RouterAddress { get; set; }= "tcp://127.0.0.1:5560";
        public  void Start()
        {

            REPProxy();
            Thread.Sleep(1000);//让代理线启动，进程内通讯先要绑定地址
            Check();
        }

        /// <summary>
        /// 启动代理，代理是阻塞的
        /// </summary>
        private  void REPProxy()
        {
            ZmqProxy.DealerAddress = DealerAddress;
            ZmqProxy.RouterAddress = RouterAddress;
            ZmqProxy.Start();
        }
        
        /// <summary>
        /// 检查使用情况
        /// </summary>
        private void  Check()
        {

            if (lstSockets.Count < 10)
            {
                if (Interlocked.Increment(ref rspNum) < 100)
                {
                    ThreadPool.QueueUserWorkItem(CreateRsp);
                }
            }
            long ticks = System.DateTime.Now.Ticks - curTikcs;
            long per = ticks / 10000+1;
            if (dicSocket.Count/per<5)
            {
                //使用频率
             
                Thread thread = new Thread(Remove);
                thread.Name = "removeRsp";
                thread.IsBackground = true;
                thread.Start();
            }
            curTikcs = DateTime.Now.Ticks;
        }

        /// <summary>
        /// 移除多余的
        /// </summary>
        private void Remove()
        {
            lock (lstSockets)
            {
                while (lstSockets.Count >10)
                {
                    var rsp = lstSockets[0];
                    lstSockets.RemoveAt(0);
                    if (rsp != null&&!rsp.IsDisposed)
                    {
                        rsp.Disconnect(DealerAddress);
                        rsp.Close();
                        rsp.Dispose();
                       
                    }
                }
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
            while (true)
            {
                try
               {
                    string client = server.ReceiveFrameString();
                    string key = random.Next(1, int.MaxValue).ToString();
                    dicSocket[key] = server;
                    lstSockets.Remove(server);
                    Check();
                    if (client != null)
                    {
                        Console.WriteLine("client:"+client);
                    }
                    if (ByteReceived != null)
                    {
                        var bytes = server.ReceiveFrameBytes();
                        var rsp = new RspSocket<byte[]> { Message = bytes, responseSocket = server, key =key, ehoServer = this };
                       
                        ByteReceived(this, rsp);

                    }
                    else if (StringReceived != null)
                    {
                        var msg = server.ReceiveFrameString();
                        var rsp = new RspSocket<string> { Message = msg, responseSocket = server, key =key, ehoServer = this };
                       
                        StringReceived(this, rsp);
                    }
                    else
                    {
                        ManualResetEventSlim resetEventSlim=new ManualResetEventSlim(false);
                        var msg = server.ReceiveFrameString();
                        var rsp = new RspSocket<string>() { responseSocket = server, Message = msg, key = key };
                        dicManualResetEvent[key] = resetEventSlim;
                        queue.Add(rsp);
                        resetEventSlim.Wait();
                    }
                }
                catch(System.Net.Sockets.SocketException ex)
                {
                    if (ex.ErrorCode == 10054)
                    {
                        break;
                    }
                }
               catch(ObjectDisposedException ex)
                {
                    if(server.IsDisposed)
                    {
                        break;
                    }
                }
            }
            Interlocked.Decrement(ref rspNum);//记录退出

        }
        
        public RspSocket<T> GetMsg<T>()
        {
            RspSocket<string> result = queue.Take();
            T obj= Util.JSONDeserializeObject<T>(result.Message);
            return new RspSocket<T>() { Message = obj,responseSocket=result.responseSocket, ehoServer = this, key=result.key };
        }

        internal void Response(string key)
        {
            if (dicSocket.TryRemove(key, out var rsp))
            {
                lstSockets.Add(rsp);
            }
            if(dicManualResetEvent.TryRemove(key,out var resetEventSlim))
            {
                resetEventSlim.Set();
            }
        }
    }
}
