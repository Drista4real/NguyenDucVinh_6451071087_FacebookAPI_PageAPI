namespace RetryService;

using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using RetryService.Models;

public class Worker : BackgroundService
{
    private readonly KafkaRetryOptions _options;
    private readonly ILogger<Worker> _logger;
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public Worker(IOptions<KafkaRetryOptions> options, ILogger<Worker> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (string.IsNullOrWhiteSpace(_options.BootstrapServers))
        {
            _logger.LogWarning("Retry worker disabled because Kafka:BootstrapServers is missing.");
            return Task.CompletedTask;
        }

        return Task.Run(() => ConsumeLoopAsync(stoppingToken), stoppingToken);
    }

    private async Task ConsumeLoopAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.GroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = ParseAutoOffsetReset(_options.AutoOffsetReset)
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            ClientId = "retry-service",
            EnableIdempotence = true,
            Acks = Acks.All
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        consumer.Subscribe(_options.SendFailedTopic);
        _logger.LogInformation(
            "Retry worker started. FailedTopic={FailedTopic} RetryTopic={RetryTopic} DeadLetterTopic={DeadLetterTopic} GroupId={GroupId}",
            _options.SendFailedTopic,
            _options.SendRetryTopic,
            _options.DeadLetterTopic,
            _options.GroupId);

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
                    _logger.LogError(ex, "Kafka consume error in retry-service.");
                    continue;
                }

                if (result?.Message?.Value is null)
                {
                    continue;
                }

                if (!TryDeserialize(result.Message.Value, out var failedMessage))
                {
                    _logger.LogWarning(
                        "Skipping invalid failed message at {TopicPartitionOffset}.",
                        result.TopicPartitionOffset);
                    consumer.Commit(result);
                    continue;
                }

                try
                {
                    if (failedMessage!.RetryCount >= _options.MaxRetries)
                    {
                        await PublishDeadLetterAsync(producer, failedMessage, stoppingToken);
                    }
                    else
                    {
                        await PublishRetryAsync(producer, failedMessage, stoppingToken);
                    }

                    consumer.Commit(result);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogError(
                        ex,
                        "Failed to process retry message at {TopicPartitionOffset}. CommandId={CommandId}",
                        result.TopicPartitionOffset,
                        failedMessage?.CommandId);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Retry worker stopping.");
        }
        finally
        {
            consumer.Close();
            producer.Flush(TimeSpan.FromSeconds(5));
        }
    }

    private async Task PublishRetryAsync(
        IProducer<string, string> producer,
        RetryMessage failedMessage,
        CancellationToken stoppingToken)
    {
        var delay = CalculateDelay(failedMessage.RetryCount);
        var nextRetry = DateTimeOffset.UtcNow.Add(delay);

        _logger.LogInformation(
            "Retrying command after delay. CommandId={CommandId} RetryCount={RetryCount} DelayMs={DelayMs}",
            failedMessage.CommandId,
            failedMessage.RetryCount,
            delay.TotalMilliseconds);

        await Task.Delay(delay, stoppingToken);

        var retryMessage = new RetryMessage
        {
            SchemaVersion = failedMessage.SchemaVersion,
            CommandId = failedMessage.CommandId,
            EventId = failedMessage.EventId,
            RetryCount = failedMessage.RetryCount + 1,
            LastError = failedMessage.LastError,
            NextRetryAt = nextRetry,
            Payload = failedMessage.Payload
        };

        await producer.ProduceAsync(
            _options.SendRetryTopic,
            new Message<string, string>
            {
                Key = retryMessage.CommandId,
                Value = JsonSerializer.Serialize(retryMessage, _jsonOptions)
            },
            stoppingToken);

        _logger.LogInformation(
            "Published retry message. CommandId={CommandId} RetryCount={RetryCount}",
            retryMessage.CommandId,
            retryMessage.RetryCount);
    }

    private async Task PublishDeadLetterAsync(
        IProducer<string, string> producer,
        RetryMessage failedMessage,
        CancellationToken stoppingToken)
    {
        var deadLetterMessage = new DeadLetterMessage
        {
            SchemaVersion = failedMessage.SchemaVersion,
            CommandId = failedMessage.CommandId,
            EventId = failedMessage.EventId,
            RetryCount = failedMessage.RetryCount,
            FailedAt = DateTimeOffset.UtcNow,
            FinalError = failedMessage.LastError ?? "Maximum retry attempts reached.",
            OriginalTopic = _options.SendFailedTopic,
            Payload = failedMessage.Payload
        };

        await producer.ProduceAsync(
            _options.DeadLetterTopic,
            new Message<string, string>
            {
                Key = deadLetterMessage.CommandId,
                Value = JsonSerializer.Serialize(deadLetterMessage, _jsonOptions)
            },
            stoppingToken);

        _logger.LogWarning(
            "Published dead letter message. CommandId={CommandId} RetryCount={RetryCount}",
            deadLetterMessage.CommandId,
            deadLetterMessage.RetryCount);
    }

    private TimeSpan CalculateDelay(int retryCount)
    {
        var exponent = Math.Max(0, retryCount);
        var seconds = _options.BaseDelaySeconds * Math.Pow(2, exponent);
        seconds = Math.Min(seconds, _options.MaxDelaySeconds);
        return TimeSpan.FromSeconds(seconds);
    }

    private bool TryDeserialize(string payload, out RetryMessage? retryMessage)
    {
        retryMessage = null;

        try
        {
            retryMessage = JsonSerializer.Deserialize<RetryMessage>(payload, _jsonOptions);
            return !string.IsNullOrWhiteSpace(retryMessage?.CommandId);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Unable to deserialize retry payload.");
            return false;
        }
    }

    private static AutoOffsetReset ParseAutoOffsetReset(string value)
    {
        return Enum.TryParse<AutoOffsetReset>(value, true, out var parsed)
            ? parsed
            : AutoOffsetReset.Earliest;
    }
}
