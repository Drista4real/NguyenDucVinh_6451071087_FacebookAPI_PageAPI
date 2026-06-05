namespace CoreService.Models;

public class KafkaOptions
{
    public string BootstrapServers { get; set; } = string.Empty;
    public string Topic { get; set; } = "raw_events";
    public string RawEventsTopic { get; set; } = string.Empty;
    public string ReplyCommandsTopic { get; set; } = "reply_commands";
    public string GroupId { get; set; } = "core-service-group";
    public string AutoOffsetReset { get; set; } = "Earliest";

    public string EffectiveRawEventsTopic =>
        string.IsNullOrWhiteSpace(RawEventsTopic) ? Topic : RawEventsTopic;
}
