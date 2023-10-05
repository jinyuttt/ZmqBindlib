using NetMQ;
using NetMQ.Sockets;

namespace ZmqBindlib
{
    /// <summary>
    /// 发布
    /// </summary>
    public class ZmqPublisher
    {
        PublisherSocket publisherSocket = null;

        /// <summary>
        /// 本地地址
        /// </summary>
        public string? LocalAddress { get; set; }

        /// <summary>
        /// 标识
        /// </summary>
        public string? PubClient { get; set; } = string.Empty;

        /// <summary>
        /// 是否使用代理，使用则LocalAddress是代理地址
        /// </summary>
        public bool IsProxy { get; set; } = false;

        public void Publish<T>(string topic, T message)
        {
            if (publisherSocket == null)
            {
                publisherSocket = new PublisherSocket();
                publisherSocket.Options.SendHighWatermark = 0;
                publisherSocket.Options.TcpKeepalive = true;
                publisherSocket.Options.HeartbeatInterval = new TimeSpan(10000);
                publisherSocket.Options.HeartbeatTimeout = new TimeSpan(1000);
                publisherSocket.Options.HeartbeatTtl = new TimeSpan(2000);
             
                if (IsProxy)
                {
                    publisherSocket.Connect(LocalAddress);
                }
                else
                {
                    publisherSocket.Bind(LocalAddress);
                }
            }
            var msg = Util.JSONSerializeObject(message);
            publisherSocket.SendMoreFrame(PubClient).SendMoreFrame(topic).SendFrame(msg);
        }

    }
}
