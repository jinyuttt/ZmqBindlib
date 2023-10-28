namespace MQBindlib
{
    internal class ConstString
    {
        public const string ReqCluster = "ReqCluster";

       /// <summary>
       /// 更新主节点
       /// </summary>
        public const string UpdateCluster = "UpdateCluster";


        public const string RspCluster = "RspCluster";

       /// <summary>
       /// 中心
       /// </summary>
        public const string PubCluster = "PubCluster";

        /// <summary>
        /// 发布方发布自己的地址
        /// </summary>
        public const string PubPublisher = "PubPublisher";

        /// <summary>
        /// 存储数据
        /// </summary>
        public const string Storage = "Storage";

        /// <summary>
        /// 心跳主题，分组模式订阅
        /// </summary>
        public const string HeartbeatTopic = "HeartbeatTopic";
    }
}
