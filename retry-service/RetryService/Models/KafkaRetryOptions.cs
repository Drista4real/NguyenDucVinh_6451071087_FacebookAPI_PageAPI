namespace RetryService.Models;

public class KafkaRetryOptions
{
    public string BootstrapServers { get; set; } = string.Empty;
    public string SendFailedTopic { get; set; } = "send_failed";
    public string SendRetryTopic { get; set; } = "send_retry";
    public string DeadLetterTopic { get; set; } = "dead_letter";
    public string GroupId { get; set; } = "retry-service-group";
    public string AutoOffsetReset { get; set; } = "Earliest";
    public int MaxRetries { get; set; } = 3;
    public int BaseDelaySeconds { get; set; } = 1;
    public int MaxDelaySeconds { get; set; } = 60;
}
