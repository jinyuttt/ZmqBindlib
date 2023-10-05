using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ZmqBindlib
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

    }
}
