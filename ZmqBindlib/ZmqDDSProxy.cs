using NetMQ;
using NetMQ.Sockets;

namespace ZmqBindlib
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

        public static void Start()
        {
            Thread thread = new Thread(DDSProxy);
            thread.Name = "ZmqProxy";
            thread.IsBackground = true;
            thread.Start();
        }
      
        private static  void DDSProxy()
        {
            using (var xpubSocket = new XPublisherSocket(PubAddress))
          
            using (var xsubSocket = new XSubscriberSocket(SubAddress))
            {
                Console.WriteLine("Intermediary started, and waiting for messages");
                // proxy messages between frontend / backend
                var proxy = new Proxy(xsubSocket, xpubSocket);
                // blocks indefinitely
                proxy.Start();
            }
        }
       
    }
}
