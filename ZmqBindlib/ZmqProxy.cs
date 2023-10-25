using NetMQ.Sockets;
using NetMQ;

namespace MQBindlib
{

    /// <summary>
    /// 请求回复代理
    /// </summary>
    internal class ZmqProxy
    {
        public static string? DealerAddress { get; set; }

        public static string? RouterAddress { get; set; }

        public static Dictionary<string,Proxy> dic=new Dictionary<string,Proxy>();

        public static Dictionary<string,bool> dic_ =new Dictionary<string, bool>();
        //private static RouterSocket routerAllSocket = null;

        //private static DealerSocket dealerAllSocket = null;

        /// <summary>
        /// 启动代理
        /// </summary>
        public static void Start(string key)
        {
            Thread thread = new Thread(REPProxy);
            thread.Name = "ZmqProxy";
            thread.IsBackground = true;
            thread.Start(key);
         
        }

       /// <summary>
       /// -1,还需要等待；
       /// 1成功
       /// 0失败
       /// </summary>
       /// <param name="key"></param>
       /// <returns></returns>
        public static int IsSucess(string key)
        {
            if(dic_.TryGetValue(key, out var val))
            {
                if (val)
                {
                    return 1;
                }
                return 0;
            }
            return -1;

        }


        /// <summary>
        /// 启动代理
        /// </summary>
        private static void REPProxy(object key)
        {
            try
            {
                var routSocket = new RouterSocket();
                routSocket.Options.ReceiveHighWatermark = 0;
                routSocket.Options.SendHighWatermark = 0;
                routSocket.Bind(RouterAddress);

                var dealSocket = new DealerSocket();
                dealSocket.Options.SendHighWatermark = 0;
                dealSocket.Options.ReceiveHighWatermark = 0;
                dealSocket.Bind(DealerAddress);
               
                Console.WriteLine("Intermediary started, and waiting for messages");
                // proxy messages between frontend / backend
                var proxy = new Proxy(routSocket, dealSocket, null);
                // blocks indefinitely
                if (key == null)
                {
                    key = Util.GuidToLongID().ToString();
                }

                dic[key.ToString()] = proxy;
                dic_[key.ToString()] = true;
                proxy.Start();
                routSocket.Close();
                dealSocket.Close();
                Console.WriteLine("Intermediary started, and waiting for messages");
            }
            catch(NetMQ.NetMQException e)
            {
                Logger.Singleton.Error("启动错误", e);
                dic_[key.ToString()] = false;
            }
        }

        /// <summary>
        /// 关闭
        /// </summary>
        /// <param name="key"></param>
        public static void Close(string key)
        {
          
            if(key!=null)
            {
                dic_.Remove(key);
                if (dic.TryGetValue(key.ToString(), out var proxy))
                {
                    proxy.Stop();
                }

            }
            else if(dic.Count==1)
            {
                foreach (var kv in dic)
                {
                    kv.Value.Stop();
                }
                dic.Clear();
                dic_.Clear();
            }

        }
      
    }
}
