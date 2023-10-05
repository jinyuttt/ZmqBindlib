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

        public void Response(string msg)
        {
            responseSocket.SendFrame(msg);
            ehoServer.Response(key);
        }
        public void Response<T>(T msg)
        {
            var obj = Util.JSONSerializeObject<T>(msg);
            responseSocket.SendFrame(obj);
            ehoServer.Response(key);
        }
        public void Close()
        {
           
            //responseSocket.Close();
        }

       
    }
}
