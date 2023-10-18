namespace MQBindlib
{
    internal class ClusterNode
    {
        /// <summary>
        /// 集群名称
        /// </summary>
        public string Name {  get; set; }=string.Empty;

        /// <summary>
        /// ID 
        /// </summary>
        public string Id { get; set; } = string.Empty;

        /// <summary>
        /// 地址
        /// </summary>
        public string Address { get; set; } = string.Empty;

        /// <summary>
        /// 订阅发布时，中心的订阅地址
        /// </summary>
        public string SubAddess {  get; set; } = string.Empty;

        /// <summary>
        /// 类别
        /// </summary>

        public NodeType NodeType { get; set; }

        public bool IsMaster {  get; set; }

      

        public bool IsClusterMaster { get; set; }

        public DateTime Value { get; set; }=DateTime.Now;
    }

    internal enum NodeType
    {
        Request,
        XPub,
        Poll
    }
}
