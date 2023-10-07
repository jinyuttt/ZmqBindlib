using NetMQ;
using NetMQ.Sockets;

namespace ZmqBindlib
{
    /// <summary>
    /// 返回结构
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class RspSocket<T>
    {
        internal ResponseSocket? responseSocket;

        internal string key=string.Empty;

        internal EhoServer ehoServer = null;
        public required T Message { get; set; }

        /// <summary>
        /// 客户端
        /// </summary>
        public string Client { get; set; }=string.Empty;

        /// <summary>
        /// 回复字符串
        /// </summary>
        /// <param name="msg"></param>
        public void Response(string msg)
        {
            responseSocket.SendFrame(msg);
            ehoServer.Response(key);
        }

        /// <summary>
        /// 回复数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msg"></param>
        public void Response<T>(T msg)
        {
            var obj = Util.JSONSerializeObject<T>(msg);
            responseSocket.SendFrame(obj);
            ehoServer.Response(key);
        }
       

       
    }
}
