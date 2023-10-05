namespace ZmqBindlib
{
    internal class InerTopicMessage
    {
        public string? Topic { get; set; }

        public string? Message { get; set; }
    }

    public class TopicMessage<T>
    {
        public string? Topic { get; set; }

        public T? Message { get; set; }
    }
}
