namespace Page_API.Models
{
    public class KafkaConsumerOptions
    {
        public string BootstrapServers { get; set; } = string.Empty;
        public string Topic { get; set; } = "reply_commands";
        public string ReplyCommandsTopic { get; set; } = "reply_commands";
        public string SendRetryTopic { get; set; } = "send_retry";
        public string SendFailedTopic { get; set; } = "send_failed";
        public string GroupId { get; set; } = "backend-api-group";
        public string AutoOffsetReset { get; set; } = "Earliest";

        public IReadOnlyList<string> EffectiveTopics
        {
            get
            {
                var topics = new[] { ReplyCommandsTopic, SendRetryTopic }
                    .Where(topic => !string.IsNullOrWhiteSpace(topic))
                    .Distinct(StringComparer.Ordinal)
                    .ToArray();

                return topics.Length == 0 ? [Topic] : topics;
            }
        }
    }
}
