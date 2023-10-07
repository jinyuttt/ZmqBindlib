using NetMQ;
using NetMQ.Sockets;
using System.Collections.Concurrent;

namespace ZmqBindlib
{

    /// <summary>
    /// 回复
    /// </summary>
    public class ZmqResponse
    {
        /// <summary>
        ///地址
        /// </summary>
        public string LocalAddress { get; set; } = String.Empty;
        ResponseSocket server = null;

        BlockingCollection<string> queue = new();


       
        /// <summary>
        /// 次优先，字符串
        /// </summary>
        public event EventHandler<string> StringReceived;



        /// <summary>
        /// 最优先，返回byte[]
        /// </summary>
        public event EventHandler<byte[]> ByteReceived;

        private void Recvice()
        {
            while (true)
            {
              string client=server.ReceiveFrameString();
                if (client != null)
                {
                    Console.WriteLine(client);
                }
                if (ByteReceived != null)
                {
                    var bytes = server.ReceiveFrameBytes();
                  
                   
                    ByteReceived(this, bytes);
                }
                else if (StringReceived != null)
                {
                    var msg = server.ReceiveFrameString();
             
                   StringReceived(this, msg);
                }
                else
                {
                    var msg = server.ReceiveFrameString();
                    queue.Add(msg);
                }


            }

        }
     
       
       
        public void Start()
        {
            Thread rec= new Thread(Recvice);
            server = new ResponseSocket();
            server.Options.Linger = new TimeSpan(10000);
            server.Bind(LocalAddress);
            rec.Name = "ZmqResponse";
            rec.Start();    
        }

        public void Stop()
        {
            server.Close();
        }

        /// <summary>
        /// 没有实现事件时使用
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public T GetMsg<T>()
        {
            string result = queue.Take();
            return Util.JSONDeserializeObject<T>(result);
        }

        public void Response(string msg)
        {
            server.SendFrame(msg);
        }
       
    }
}

