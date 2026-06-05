using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Page_API.Models;

namespace Page_API.Services
{
    public class FacebookCommandConsumerService : BackgroundService
    {
        private readonly KafkaConsumerOptions _options;
        private readonly IServiceScopeFactory _serviceScopeFactory;
        private readonly ILogger<FacebookCommandConsumerService> _logger;
        private readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        public FacebookCommandConsumerService(
            IOptions<KafkaConsumerOptions> options,
            IServiceScopeFactory serviceScopeFactory,
            ILogger<FacebookCommandConsumerService> logger)
        {
            _options = options.Value;
            _serviceScopeFactory = serviceScopeFactory;
            _logger = logger;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (string.IsNullOrWhiteSpace(_options.BootstrapServers)
                || string.IsNullOrWhiteSpace(_options.GroupId))
            {
                _logger.LogWarning("Kafka consumer is disabled because KafkaConsumer configuration is incomplete.");
                return Task.CompletedTask;
            }

            return Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);
        }

        private void ConsumeLoop(CancellationToken stoppingToken)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                GroupId = _options.GroupId,
                EnableAutoCommit = false,
                AutoOffsetReset = ParseAutoOffsetReset(_options.AutoOffsetReset)
            };

            using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
            consumer.Subscribe(_options.EffectiveTopics);

            _logger.LogInformation(
                "Kafka consumer started. Topics={Topics} GroupId={GroupId} BootstrapServers={BootstrapServers}",
                string.Join(",", _options.EffectiveTopics),
                _options.GroupId,
                _options.BootstrapServers);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    ConsumeResult<string, string>? consumeResult;
                    try
                    {
                        consumeResult = consumer.Consume(stoppingToken);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Kafka consume error.");
                        continue;
                    }

                    if (consumeResult?.Message?.Value is null)
                    {
                        continue;
                    }

                    if (!TryBuildCommand(
                            consumeResult.Topic,
                            consumeResult.Message.Value,
                            out var command,
                            out var retryCount))
                    {
                        _logger.LogWarning(
                            "Skipping invalid command at {TopicPartitionOffset}.",
                            consumeResult.TopicPartitionOffset);
                        consumer.Commit(consumeResult);
                        continue;
                    }

                    try
                    {
                        using var scope = _serviceScopeFactory.CreateScope();
                        var handler = scope.ServiceProvider.GetRequiredService<IFacebookCommandHandler>();
                        handler.HandleAsync(command!, retryCount, stoppingToken).GetAwaiter().GetResult();

                        consumer.Commit(consumeResult);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(
                            ex,
                            "Failed to process command at {TopicPartitionOffset}. CommandId={CommandId}",
                            consumeResult.TopicPartitionOffset,
                            command?.CommandId);

                        if (IsPermanentCommandFailure(ex))
                        {
                            consumer.Commit(consumeResult);
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Kafka consumer stopping.");
            }
            finally
            {
                consumer.Close();
            }
        }

        private bool TryBuildCommand(
            string topic,
            string payload,
            out ReplyCommand? command,
            out int retryCount)
        {
            command = null;
            retryCount = 0;

            try
            {
                if (string.Equals(topic, _options.SendRetryTopic, StringComparison.Ordinal))
                {
                    var retryMessage = JsonSerializer.Deserialize<RetryMessage>(payload, _jsonOptions);
                    command = retryMessage?.Payload;
                    retryCount = retryMessage?.RetryCount ?? 0;
                }
                else
                {
                    command = JsonSerializer.Deserialize<ReplyCommand>(payload, _jsonOptions);
                }

                if (command is null
                    || string.IsNullOrWhiteSpace(command.CommandId)
                    || string.IsNullOrWhiteSpace(command.Action))
                {
                    return false;
                }

                if (string.IsNullOrWhiteSpace(command.Target.CommentId)
                    && !string.Equals(command.Action, "manual_review", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }

                return true;
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(ex, "Unable to deserialize command payload.");
                return false;
            }
        }

        private static AutoOffsetReset ParseAutoOffsetReset(string value)
        {
            if (Enum.TryParse<AutoOffsetReset>(value, true, out var parsed))
            {
                return parsed;
            }

            return AutoOffsetReset.Earliest;
        }

        private static bool IsPermanentCommandFailure(Exception exception)
        {
            if (exception is InvalidOperationException)
            {
                return true;
            }

            if (exception is FacebookApiException facebookApiException)
            {
                var statusCode = (int)facebookApiException.UpstreamStatusCode;
                return statusCode >= StatusCodes.Status400BadRequest
                    && statusCode != StatusCodes.Status429TooManyRequests
                    && statusCode < StatusCodes.Status500InternalServerError;
            }

            return false;
        }
    }
}
