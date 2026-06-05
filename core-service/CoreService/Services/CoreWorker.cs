using System.Text.Json;
using Confluent.Kafka;
using CoreService.Models;
using Microsoft.Extensions.Options;

namespace CoreService.Services;

public class CoreWorker : BackgroundService
{
    private readonly KafkaOptions _kafkaOptions;
    private readonly AnalysisOptions _analysisOptions;
    private readonly IEventAnalyzer _eventAnalyzer;
    private readonly AutomationRuleEngine _ruleEngine;
    private readonly ILogger<CoreWorker> _logger;
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public CoreWorker(
        IOptions<KafkaOptions> kafkaOptions,
        IOptions<AnalysisOptions> analysisOptions,
        IEventAnalyzer eventAnalyzer,
        AutomationRuleEngine ruleEngine,
        ILogger<CoreWorker> logger)
    {
        _kafkaOptions = kafkaOptions.Value;
        _analysisOptions = analysisOptions.Value;
        _eventAnalyzer = eventAnalyzer;
        _ruleEngine = ruleEngine;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (string.IsNullOrWhiteSpace(_kafkaOptions.BootstrapServers))
        {
            _logger.LogWarning("Core worker disabled because Kafka:BootstrapServers is missing.");
            return Task.CompletedTask;
        }

        return Task.Run(() => ConsumeLoopAsync(stoppingToken), stoppingToken);
    }

    private async Task ConsumeLoopAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = _kafkaOptions.GroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = ParseAutoOffsetReset(_kafkaOptions.AutoOffsetReset)
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            ClientId = "core-service",
            EnableIdempotence = true,
            Acks = Acks.All
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        consumer.Subscribe(_kafkaOptions.EffectiveRawEventsTopic);
        _logger.LogInformation(
            "Core worker started. RawTopic={RawTopic} ReplyTopic={ReplyTopic} GroupId={GroupId}",
            _kafkaOptions.EffectiveRawEventsTopic,
            _kafkaOptions.ReplyCommandsTopic,
            _kafkaOptions.GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result;
                try
                {
                    result = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Kafka consume error in core-service.");
                    continue;
                }

                if (result?.Message?.Value is null)
                {
                    continue;
                }

                if (!TryDeserializeEvent(result.Message.Value, out var facebookEvent))
                {
                    _logger.LogWarning(
                        "Skipping invalid raw event at {TopicPartitionOffset}.",
                        result.TopicPartitionOffset);
                    consumer.Commit(result);
                    continue;
                }

                try
                {
                    var analysis = await AnalyzeWithFallbackAsync(facebookEvent!, stoppingToken);
                    var command = await _ruleEngine.BuildCommandAsync(
                        facebookEvent!,
                        analysis,
                        stoppingToken);

                    if (command is not null)
                    {
                        var payload = JsonSerializer.Serialize(command, _jsonOptions);
                        await producer.ProduceAsync(
                            _kafkaOptions.ReplyCommandsTopic,
                            new Message<string, string>
                            {
                                Key = command.Target.CommentId,
                                Value = payload
                            },
                            stoppingToken);

                        _logger.LogInformation(
                            "Published reply command. CommandId={CommandId} EventId={EventId} Action={Action} Intent={Intent} Sentiment={Sentiment}",
                            command.CommandId,
                            command.EventId,
                            command.Action,
                            command.Intent,
                            command.Sentiment);
                    }

                    consumer.Commit(result);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogError(
                        ex,
                        "Failed to process raw event at {TopicPartitionOffset}. EventId={EventId}",
                        result.TopicPartitionOffset,
                        facebookEvent?.EventId);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Core worker stopping.");
        }
        finally
        {
            consumer.Close();
            producer.Flush(TimeSpan.FromSeconds(5));
        }
    }

    private async Task<AnalysisResult> AnalyzeWithFallbackAsync(
        NormalizedFacebookEvent facebookEvent,
        CancellationToken stoppingToken)
    {
        var timeoutSeconds = Math.Max(1, _analysisOptions.TimeoutSeconds);
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));

        try
        {
            return await _eventAnalyzer.AnalyzeAsync(facebookEvent, timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogWarning(
                "AI analysis timed out after {TimeoutSeconds}s. EventId={EventId}",
                timeoutSeconds,
                facebookEvent.EventId);
            return AnalysisResult.Fallback("analysis_timeout");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AI analysis failed. EventId={EventId}", facebookEvent.EventId);
            return AnalysisResult.Fallback("analysis_error");
        }
    }

    private bool TryDeserializeEvent(string payload, out NormalizedFacebookEvent? facebookEvent)
    {
        facebookEvent = null;

        try
        {
            using var document = JsonDocument.Parse(payload);
            var root = document.RootElement;

            facebookEvent = new NormalizedFacebookEvent
            {
                EventId = GetString(root, "EventId", "event_id") ?? string.Empty,
                Source = GetString(root, "Source", "source") ?? "facebook",
                Object = GetString(root, "Object", "object") ?? "page",
                EventType = GetString(root, "EventType", "event_type") ?? "comment_created",
                PageId = GetString(root, "PageId", "page_id") ?? string.Empty,
                PostId = GetString(root, "PostId", "post_id"),
                CommentId = GetString(root, "CommentId", "comment_id"),
                ActorId = GetString(root, "ActorId", "actor_id", "user_id"),
                Message = GetString(root, "Message", "message"),
                CreatedAt = GetDateTimeOffset(root, "CreatedAt", "created_at") ?? DateTimeOffset.UtcNow,
                ReceivedAt = GetDateTimeOffset(root, "ReceivedAt", "received_at") ?? DateTimeOffset.UtcNow,
                SchemaVersion = GetString(root, "SchemaVersion", "schema_version") ?? "1.0"
            };

            return !string.IsNullOrWhiteSpace(facebookEvent.EventId);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Unable to deserialize raw event payload.");
            return false;
        }
    }

    private static string? GetString(JsonElement root, params string[] names)
    {
        foreach (var name in names)
        {
            if (root.TryGetProperty(name, out var value))
            {
                return value.ValueKind switch
                {
                    JsonValueKind.String => value.GetString(),
                    JsonValueKind.Number => value.GetRawText(),
                    _ => value.ToString()
                };
            }
        }

        return null;
    }

    private static DateTimeOffset? GetDateTimeOffset(JsonElement root, params string[] names)
    {
        var value = GetString(root, names);
        return DateTimeOffset.TryParse(value, out var parsed) ? parsed : null;
    }

    private static AutoOffsetReset ParseAutoOffsetReset(string value)
    {
        return Enum.TryParse<AutoOffsetReset>(value, true, out var parsed)
            ? parsed
            : AutoOffsetReset.Earliest;
    }
}
