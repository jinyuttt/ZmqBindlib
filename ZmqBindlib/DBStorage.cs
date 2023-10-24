using LiteDB;
using System.Collections.Concurrent;

namespace MQBindlib
{
    internal class DBStorage
    {
        LiteDatabase db = null;

        BlockingCollection<InerTopicMessage> messages = new BlockingCollection<InerTopicMessage>();

        public DBStorage()
        {
             db = new LiteDatabase(@"zmqbindlib.db");
            start();
        }

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
        public void Add(string key, string value)
        {
            messages.Add(new InerTopicMessage() { Topic = key, Message = value, DateValue = System.DateTime.Now.Ticks });
        }

        public void Remove(string key)
        {
            var col = db.GetCollection<InerTopicMessage>(key);
            col.DeleteMany(x => x.Topic == key);
        }

        public List<InerTopicMessage> GetMessage(string key)
        {
            var col = db.GetCollection<InerTopicMessage>(key);
            return col.Query().Where(X => X.Topic == key).OrderBy(X=>X.DateValue).ToList();
        }

        public void Remove(string key,long offset)
        {
            var col = db.GetCollection<InerTopicMessage>(key);
            col.DeleteMany(x => x.Topic == key&&x.DateValue<offset);
        }

    }
}
