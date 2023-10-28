using LiteDB;
using System.Collections.Concurrent;

namespace MQBindlib
{

    /// <summary>
    /// 存数据
    /// </summary>
    internal class DBStorage
    {
        LiteDatabase db = null;

        BlockingCollection<InerTopicMessage> messages = new BlockingCollection<InerTopicMessage>();

        public DBStorage()
        {
             db = new LiteDatabase(@"zmqbindlib.db");
            start();
        }

        /// <summary>
        /// 启动存储
        /// </summary>
        private void start()
        {
            Task.Factory.StartNew(() =>
            {
                while(true)
                {
                   var msg= messages.Take();
                    var col = db.GetCollection<InerTopicMessage>(msg.Topic);
                    col.Insert(msg);
                }
            });
        }

        /// <summary>
        /// 添加数据
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void Add(string key, string value)
        {
            messages.Add(new InerTopicMessage() { Topic = key, Message = value, DateValue = System.DateTime.Now.Ticks });
        }


        /// <summary>
        /// 删除数据
        /// </summary>
        /// <param name="key"></param>
        public void Remove(string key)
        {
            var col = db.GetCollection<InerTopicMessage>(key);
            col.DeleteMany(x => x.Topic == key);
        }

        /// <summary>
        /// 获取数据
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public List<InerTopicMessage> GetMessage(string key)
        {
            var col = db.GetCollection<InerTopicMessage>(key);
            return col.Query().Where(X => X.Topic == key).OrderBy(X=>X.DateValue).ToList();
        }

        /// <summary>
        /// 移除一段数据数据
        /// </summary>
        /// <param name="key"></param>
        /// <param name="offset"></param>
        public void Remove(string key,long offset)
        {
            var col = db.GetCollection<InerTopicMessage>(key);
            col.DeleteMany(x => x.Topic == key&&x.DateValue<offset);
        }

    }
}
