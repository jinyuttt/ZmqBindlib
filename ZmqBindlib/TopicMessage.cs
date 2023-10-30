namespace MQBindlib
{
    internal class InerTopicMessage
    {
        public string? Topic { get; set; }

        public string? Message { get; set; }

        public string PubClient { get; set; }

        public long DateValue { get; set; }
    }

    public class TopicMessage<T>
    {
        /// <summary>
        /// 主题
        /// </summary>
        public string? Topic { get; set; }

        /// <summary>
        /// 消息
        /// </summary>
        public T? Message { get; set; }

        /// <summary>
        /// 发布端标识
        /// </summary>
        public string PubClient { get; set; }   
    }
}
