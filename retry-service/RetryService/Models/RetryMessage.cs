using System.Text.Json;
using System.Text.Json.Serialization;

namespace RetryService.Models;

public class RetryMessage
{
    [JsonPropertyName("schema_version")]
    public int SchemaVersion { get; set; } = 1;

    [JsonPropertyName("command_id")]
    public string CommandId { get; set; } = string.Empty;

    [JsonPropertyName("event_id")]
    public string EventId { get; set; } = string.Empty;

    [JsonPropertyName("retry_count")]
    public int RetryCount { get; set; }

    [JsonPropertyName("last_error")]
    public string? LastError { get; set; }

    [JsonPropertyName("next_retry_at")]
    public DateTimeOffset? NextRetryAt { get; set; }

    [JsonPropertyName("payload")]
    public JsonElement Payload { get; set; }
}
