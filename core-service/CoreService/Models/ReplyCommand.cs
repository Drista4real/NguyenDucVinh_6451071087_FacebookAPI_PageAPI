using System.Text.Json.Serialization;

namespace CoreService.Models;

public class ReplyCommand
{
    [JsonPropertyName("schema_version")]
    public int SchemaVersion { get; set; } = 1;

    [JsonPropertyName("command_id")]
    public string CommandId { get; set; } = string.Empty;

    [JsonPropertyName("event_id")]
    public string EventId { get; set; } = string.Empty;

    [JsonPropertyName("action")]
    public string Action { get; set; } = "reply";

    [JsonPropertyName("target")]
    public ReplyTarget Target { get; set; } = new();

    [JsonPropertyName("reply_text")]
    public string ReplyText { get; set; } = string.Empty;

    [JsonPropertyName("intent")]
    public string Intent { get; set; } = "unknown";

    [JsonPropertyName("sentiment")]
    public string Sentiment { get; set; } = "neutral";

    [JsonPropertyName("created_at")]
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
}

public class ReplyTarget
{
    [JsonPropertyName("page_id")]
    public string PageId { get; set; } = string.Empty;

    [JsonPropertyName("comment_id")]
    public string CommentId { get; set; } = string.Empty;
}
