using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Page_API.Models;

namespace Page_API.Services
{
    public class KafkaFailurePublisher : IKafkaFailurePublisher, IDisposable
    {
        private readonly KafkaConsumerOptions _options;
        private readonly IProducer<string, string> _producer;
        private readonly ILogger<KafkaFailurePublisher> _logger;
        private readonly JsonSerializerOptions _jsonOptions = new()
        {
            PropertyNameCaseInsensitive = true
        };

        public KafkaFailurePublisher(
            IOptions<KafkaConsumerOptions> options,
            ILogger<KafkaFailurePublisher> logger)
        {
            _options = options.Value;
            _logger = logger;

            if (string.IsNullOrWhiteSpace(_options.BootstrapServers))
            {
                throw new InvalidOperationException("KafkaConsumer:BootstrapServers is not configured.");
            }

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                ClientId = "backend-api",
                EnableIdempotence = true,
                Acks = Acks.All
            };

            _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        }

        public async Task PublishAsync(
            ReplyCommand command,
            int retryCount,
            string lastError,
            CancellationToken cancellationToken)
        {
            var message = new RetryMessage
            {
                SchemaVersion = 1,
                CommandId = command.CommandId,
                EventId = command.EventId,
                RetryCount = retryCount,
                LastError = lastError,
                NextRetryAt = null,
                Payload = command
            };

            var delivery = await _producer.ProduceAsync(
                _options.SendFailedTopic,
                new Message<string, string>
                {
                    Key = command.CommandId,
                    Value = JsonSerializer.Serialize(message, _jsonOptions)
                },
                cancellationToken);

            _logger.LogWarning(
                "Published send_failed. CommandId={CommandId} Topic={Topic} Offset={Offset}",
                command.CommandId,
                delivery.Topic,
                delivery.Offset.Value);
        }

        public void Dispose()
        {
            _producer.Flush(TimeSpan.FromSeconds(5));
            _producer.Dispose();
        }
    }
}
