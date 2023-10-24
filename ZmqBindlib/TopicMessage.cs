namespace MQBindlib
{
    internal class InerTopicMessage
    {
        public string? Topic { get; set; }

        public string? Message { get; set; }

        public long DateValue { get; set; }
    }

    public class TopicMessage<T>
    {
        public string? Topic { get; set; }

        public T? Message { get; set; }
    }
}
