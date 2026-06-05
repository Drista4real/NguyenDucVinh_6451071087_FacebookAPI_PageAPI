using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace WebhookService.Models;

public class NormalizedFacebookEvent
{
    [JsonProperty("schema_version")]
    public int SchemaVersion { get; set; } = 1;

    [JsonProperty("event_id")]
    public string EventId { get; set; } = string.Empty;

    [JsonProperty("source")]
    public string Source { get; set; } = "facebook";

    [JsonProperty("object")]
    public string Object { get; set; } = "page";

    [JsonProperty("event_type")]
    public string EventType { get; set; } = "comment_created";

    [JsonProperty("page_id")]
    public string PageId { get; set; } = string.Empty;

    [JsonProperty("post_id")]
    public string? PostId { get; set; }

    [JsonProperty("comment_id")]
    public string? CommentId { get; set; }

    [JsonProperty("actor_id")]
    public string? ActorId { get; set; }

    [JsonProperty("message")]
    public string? Message { get; set; }

    [JsonProperty("created_at")]
    public DateTimeOffset CreatedAt { get; set; }

    [JsonProperty("raw_event")]
    public JObject RawEvent { get; set; } = new();

    [JsonProperty("received_at")]
    public DateTimeOffset ReceivedAt { get; set; }
}
