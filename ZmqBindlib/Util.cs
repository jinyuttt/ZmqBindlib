using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace MQBindlib
{
    internal class Util
    {
     
        public static string JSONSerializeObject<T>(T obj)
        {
            var json = System.Text.Json.JsonSerializer.Serialize(obj);
            return json;
        }

        public static T JSONDeserializeObject<T>(string json)
        {
            var obj = System.Text.Json.JsonSerializer.Deserialize<T>(json);
            return obj;
        }

        /// <summary>
        /// 获取通讯
        /// </summary>
        /// <returns></returns>
        public static string GetLocalIP()
        {
            string localIP = string.Empty;
            using (Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, 0))
            {
                socket.Connect("8.8.8.8", 65530);
                IPEndPoint endPoint = socket.LocalEndPoint as IPEndPoint;
                localIP = endPoint.Address.ToString();
            }
            return localIP;
        }

        /// <summary>
        /// 获取所有
        /// </summary>
        /// <returns></returns>
        public static List<string> GetLocalAllIP()
        {
            List<string> list = new List<string>();
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    list.Add(ip.ToString());
                }
            }
            return list;
        }

        /// <summary>  
        /// 根据GUID获取16位的唯一字符串  
        /// </summary>  
        /// <param name=\"guid\"></param>  
        /// <returns></returns>  
        public static string GuidTo16String()
        {
            long i = 1;
            foreach (byte b in Guid.NewGuid().ToByteArray())
                i *= ((int)b + 1);
            return string.Format("{0:x}", i - DateTime.Now.Ticks);  
        }
        /// <summary>  
        /// 根据GUID获取19位的唯一数字序列  
        /// </summary>  
        /// <returns></returns>  
        public static long GuidToLongID()
        {
            byte[] buffer = Guid.NewGuid().ToByteArray();
            return BitConverter.ToInt64(buffer, 0);
        }


    }
}
