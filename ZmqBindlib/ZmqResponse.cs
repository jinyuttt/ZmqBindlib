using NetMQ;
using NetMQ.Sockets;
using System.Collections.Concurrent;

namespace MQBindlib
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

        BlockingCollection<RequestMsg> queue = new();



        /// <summary>
        /// 次优先，字符串，第一个参数客户端标识
        /// </summary>
        public event Action<string,string,ZmqResponse> StringReceived;



        /// <summary>
        /// 最优先，返回byte[],第一个参数客户端标识
        /// </summary>
        public event Action<string,byte[],ZmqResponse> ByteReceived;



        /// <summary>
        /// 接收数据
        /// </summary>
        private void Recvice()
        {
            while (true)
            {
               string client=server.ReceiveFrameString();
               
                if (ByteReceived != null)
                {
                    var bytes = server.ReceiveFrameBytes();
                  
                   
                    ByteReceived(client, bytes,this);
                }
                else if (StringReceived != null)
                {
                    var msg = server.ReceiveFrameString();
             
                   StringReceived(client,msg,this);
                }
                else
                {
                    var msg = server.ReceiveFrameString();
                    queue.Add(new RequestMsg() { ClientFlage=client, Msg=msg});
                }


            }

        }
     
       
       
        /// <summary>
        /// 开始
        /// </summary>
        public void Start()
        {
            Thread rec= new Thread(Recvice);
            server = new ResponseSocket();
            server.Options.Linger = new TimeSpan(10000);
            server.Bind(LocalAddress);
            rec.Name = "ZmqResponse";
            rec.Start();    
        }

        /// <summary>
        /// 停止
        /// </summary>
        public void Stop()
        {
            server.Close();
        }

        /// <summary>
        /// 没有实现事件时使用
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public T GetMsg<T>(out string clientFlage)
        {
            var result = queue.Take();
            clientFlage = result.ClientFlage;
            return Util.JSONDeserializeObject<T>(result.Msg);
        }

        /// <summary>
        /// 回复结果
        /// </summary>
        /// <param name="msg"></param>
        public void Response(string msg)
        {
            server.SendFrame(msg);
        }
       
        
    }
    class RequestMsg
    {
        public RequestMsg() { }

        public string Msg { get; set; }

        public string ClientFlage { get; set; }
    }
}

