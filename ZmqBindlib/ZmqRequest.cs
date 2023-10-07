using NetMQ;
using NetMQ.Sockets;

namespace ZmqBindlib
{

    /// <summary>
    /// 请求
    /// </summary>
    public  class ZmqRequest
    {
        /// <summary>
        /// 远端地址
        /// </summary>
        public string RemoteAddress { get; set; }=String.Empty;

        private RequestSocket requestSocket = null;

        /// <summary>
        /// 标识
        /// </summary>
        public string? Client { get; set; } = string.Empty;


        /// <summary>
        /// 请求
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        public string Request(string msg)
        {
          
            using (var client = new RequestSocket(RemoteAddress))  // connect
            { 
                // Send a message from the client socket
                client.SendMoreFrame(Client).SendFrame(msg);
                return client.ReceiveFrameString();
            
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
                client.Options.Identity = System.Text.Encoding.UTF8.GetBytes(Client);
                // Send a message from the client socket
                client.SendMoreFrame(Client).SendFrame(msg);
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
                client.SendMoreFrame(Client).SendFrame(obj);
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
              
            }

            var obj = Util.JSONSerializeObject(msg);
            requestSocket.SendMoreFrame(Client).SendFrame(obj);
            var rsp = requestSocket.ReceiveFrameString();
            if (typeof(T) == typeof(string))
            {
                return (T)Convert.ChangeType(rsp, typeof(T));
            }
            var result = Util.JSONDeserializeObject<T>(rsp);
            return result;
        }

       
    }
}
