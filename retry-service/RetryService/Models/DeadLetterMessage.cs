using System.Text.Json;
using System.Text.Json.Serialization;

namespace RetryService.Models;

public class DeadLetterMessage
{
    [JsonPropertyName("schema_version")]
    public int SchemaVersion { get; set; } = 1;

    [JsonPropertyName("command_id")]
    public string CommandId { get; set; } = string.Empty;

    [JsonPropertyName("event_id")]
    public string EventId { get; set; } = string.Empty;

    [JsonPropertyName("retry_count")]
    public int RetryCount { get; set; }

    [JsonPropertyName("failed_at")]
    public DateTimeOffset FailedAt { get; set; } = DateTimeOffset.UtcNow;

    [JsonPropertyName("final_error")]
    public string FinalError { get; set; } = string.Empty;

    [JsonPropertyName("original_topic")]
    public string OriginalTopic { get; set; } = "send_failed";

    [JsonPropertyName("payload")]
    public JsonElement Payload { get; set; }
}
