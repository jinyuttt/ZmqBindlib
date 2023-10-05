using NetMQ.Sockets;
using NetMQ;

namespace ZmqBindlib
{

    /// <summary>
    /// 请求恢复代理
    /// </summary>
    internal class ZmqProxy
    {
        public static string? DealerAddress { get; set; }

        public static string? RouterAddress { get; set; }

        /// <summary>
        /// 启动代理
        /// </summary>
        public static void Start()
        {
            Thread thread = new Thread(REPProxy);
            thread.Name = "ZmqProxy";
            thread.IsBackground = true;
            thread.Start();
        }

        /// <summary>
        /// 启动代理
        /// </summary>
        private static void REPProxy()
        {
            var routSocket = new RouterSocket();
            routSocket.Options.ReceiveHighWatermark = 0;
            routSocket.Options.SendHighWatermark    = 0;
            routSocket.Bind(RouterAddress);

            var dealSocket = new DealerSocket();
            dealSocket.Options.SendHighWatermark = 0;
            dealSocket.Options.ReceiveHighWatermark  = 0;
            dealSocket.Bind(DealerAddress);

            var pubSocket = new PublisherSocket();
            Console.WriteLine("Intermediary started, and waiting for messages");
                // proxy messages between frontend / backend
                var proxy = new Proxy(routSocket, dealSocket, pubSocket);
                // blocks indefinitely
                proxy.Start();
            Console.WriteLine("Intermediary started, and waiting for messages");
        }
    }
}
