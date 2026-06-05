using Page_API.Models;

namespace Page_API.Services
{
    public class FacebookCommandHandler : IFacebookCommandHandler
    {
        private readonly IFacebookService _facebookService;
        private readonly IProcessedCommandStore _processedCommandStore;
        private readonly IKafkaFailurePublisher _failurePublisher;
        private readonly ILogger<FacebookCommandHandler> _logger;

        public FacebookCommandHandler(
            IFacebookService facebookService,
            IProcessedCommandStore processedCommandStore,
            IKafkaFailurePublisher failurePublisher,
            ILogger<FacebookCommandHandler> logger)
        {
            _facebookService = facebookService;
            _processedCommandStore = processedCommandStore;
            _failurePublisher = failurePublisher;
            _logger = logger;
        }

        public async Task HandleAsync(
            ReplyCommand command,
            int retryCount,
            CancellationToken cancellationToken)
        {
            var idempotencyKey = BuildIdempotencyKey(command);
            if (await _processedCommandStore.IsProcessedAsync(idempotencyKey, cancellationToken))
            {
                _logger.LogInformation(
                    "Skipping already processed command. CommandId={CommandId} Action={Action}",
                    command.CommandId,
                    command.Action);
                return;
            }

            try
            {
                await ExecuteCommandAsync(command, cancellationToken);
                await _processedCommandStore.MarkProcessedAsync(
                    idempotencyKey,
                    command.CommandId,
                    command.Action,
                    cancellationToken);

                _logger.LogInformation(
                    "Command processed. CommandId={CommandId} Action={Action} EventId={EventId}",
                    command.CommandId,
                    command.Action,
                    command.EventId);
            }
            catch (Exception ex) when (ShouldRetry(ex))
            {
                _logger.LogWarning(
                    ex,
                    "Temporary command failure. Publishing to send_failed. CommandId={CommandId} RetryCount={RetryCount}",
                    command.CommandId,
                    retryCount);

                await _failurePublisher.PublishAsync(command, retryCount, ex.Message, cancellationToken);
            }
        }

        private async Task ExecuteCommandAsync(ReplyCommand command, CancellationToken cancellationToken)
        {
            switch (command.Action.Trim().ToLowerInvariant())
            {
                case "reply":
                    if (string.IsNullOrWhiteSpace(command.ReplyText))
                    {
                        throw new InvalidOperationException("Reply command requires reply_text.");
                    }

                    await _facebookService.ReplyToCommentAsync(
                        command.Target.CommentId,
                        command.ReplyText,
                        cancellationToken);
                    break;

                case "hide":
                case "blacklist":
                    await _facebookService.HideCommentAsync(command.Target.CommentId, cancellationToken);
                    break;

                case "manual_review":
                    _logger.LogInformation(
                        "Command requires manual review. CommandId={CommandId} CommentId={CommentId} Intent={Intent} Sentiment={Sentiment}",
                        command.CommandId,
                        command.Target.CommentId,
                        command.Intent,
                        command.Sentiment);
                    break;

                default:
                    throw new InvalidOperationException($"Unsupported command action: {command.Action}");
            }
        }

        private static string BuildIdempotencyKey(ReplyCommand command)
        {
            return $"{command.CommandId}:{command.Action}".ToLowerInvariant();
        }

        private static bool ShouldRetry(Exception exception)
        {
            if (exception is TaskCanceledException or HttpRequestException)
            {
                return true;
            }

            if (exception is FacebookApiException facebookApiException)
            {
                var statusCode = (int)facebookApiException.UpstreamStatusCode;
                return statusCode == StatusCodes.Status429TooManyRequests
                    || statusCode >= StatusCodes.Status500InternalServerError;
            }

            return false;
        }
    }
}
