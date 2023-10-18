using System.Collections.Generic;

namespace MQBindlib
{
    /// <summary>
    /// 中心信息
    /// </summary>
    internal class Cluster
    {
       public  static   List<ClusterNode> clusters=new List<ClusterNode>();

        // Dead nodes timeout
        private readonly static TimeSpan m_deadNodeTimeout = TimeSpan.FromSeconds(10);

        public static ZmqBus bus;

        /// <summary>
        /// 发布方等待消息地址
        /// </summary>
        private readonly static List<string> channels = new List<string>();

        /// <summary>
        /// 添加节点
        /// </summary>
        /// <param name="node"></param>
        public static void Add(ClusterNode node)
        {
            lock (clusters)
            {
                var r = clusters.Find(p => p.Id == node.Id);
                if (r == null)
                {

                    clusters.Add(node);
                    Console.WriteLine(string.Format("add node id:{0},name:{1},address:{2}", node.Id, node.Name, node.Address));
                    if (node.IsClusterMaster)
                    {
                        //加入指定主节点
                        node.IsMaster = true;
                        var item = clusters.Find(p => p.IsMaster && !p.IsClusterMaster);
                        if (item != null)
                        {
                            item.IsMaster = false;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 获取节点
        /// </summary>
        /// <param name="name"></param>
        /// <param name="nodeType"></param>
        /// <returns></returns>
        public static List<ClusterNode> GetNodes(string name,NodeType nodeType)
        {
          //  Console.WriteLine($"获取:{name}");
            var lst=  clusters.FindAll(p => p.Name == name&&p.NodeType==nodeType).ToList();
            var master= lst.Find(p => p.IsMaster);
            if (master == null)
            {
                Dictionary<string, NodeType> dic = new Dictionary<string, NodeType>();
                dic[name] = nodeType;
                Update(dic);
            }
            return lst;
        }

        /// <summary>
        /// 刷新节点
        /// </summary>
        /// <param name="id"></param>
        public static void Flush(string id)
        {
            var node = clusters.Find(p => p.Id == id);
            if (node != null)
            {
                node.Value = DateTime.Now;
            }
        }

        /// <summary>
        /// 移除节点
        /// </summary>
        public static void Remove()
        {
            Thread rv = new Thread(p =>
            {
                while (true)
                {
                    Thread.Sleep(1000);
                    var keys = clusters.Where(p => DateTime.Now > p.Value + m_deadNodeTimeout).ToArray();
                    Dictionary<string, NodeType> dic = new();
                    foreach (var node in keys)
                    {
                        lock (clusters)
                        {
                            clusters.Remove(node);
                            Console.WriteLine(string.Format("remove node id:{0},name:{1},address:{2}", node.Id, node.Name, node.Address));
                            if (node.IsMaster)
                            {
                                dic[node.Name] = node.NodeType;
                            }
                        }
                    }
                    Update(dic);
                }
            });
            rv.Start();
        }

        /// <summary>
        /// 更新节点
        /// </summary>
        /// <param name="dic"></param>
        private  static void Update(Dictionary<string,NodeType> dic)
        {
            //通知集群；
            lock (clusters)
            {
                foreach (var key in dic.Keys)
                {
                    var lst = clusters.FindAll(p => p.Name == key && p.NodeType == dic[key]).ToList();
                    if (lst != null && lst.Count > 0)
                    {
                        if (lst.Find(p => p.IsMaster) == null)
                        {
                            var node = lst.OrderByDescending(p => p.Id).First();
                            node.IsMaster = true;
                            Console.WriteLine($"集群:{node.Name},节点:{node.Id},地址:{node.Address}转Master");
                            if (bus != null)
                            {
                                bus.Publish(ConstString.UpdateCluster, node.Id);
                            }
                        }
                    }
                }
            }

        }

        /// <summary>
        /// 更新Master
        /// </summary>
        /// <param name="id"></param>
        public static void UPdateMaster(string id)
        {
            var node = clusters.Find(p => p.Id == id);
            if (node != null)
            {
                node.IsMaster = true;
                //其余修改
                var item = clusters.FindAll(p => p.Name == node.Name && p.NodeType == node.NodeType&&p.Id!=id);
                if (item != null)
                {
                    foreach (var item1 in item)
                    {
                        item1.IsMaster = false;
                    }
                }
            }
        }

        /// <summary>
        /// 添加发布方
        /// </summary>
        /// <param name="pubRsp"></param>
        public static void AddRsp(string pubRsp)
        {
            if (!channels.Contains(pubRsp))
            {
                if(!pubRsp.StartsWith("tcp"))
                {
                    Console.WriteLine("dd");
                    return;
                }
                channels.Add(pubRsp);
            }
        }

        /// <summary>
        /// 发布方
        /// </summary>
        /// <returns></returns>
        public static string[] GetRsp()
        {
            string[] ret = new string[channels.Count];
             channels.CopyTo(ret);
            return ret;
        }
    }


}
