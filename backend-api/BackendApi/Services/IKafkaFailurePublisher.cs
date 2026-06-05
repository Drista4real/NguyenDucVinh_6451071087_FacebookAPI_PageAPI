using Page_API.Models;

namespace Page_API.Services
{
    public interface IKafkaFailurePublisher
    {
        Task PublishAsync(
            ReplyCommand command,
            int retryCount,
            string lastError,
            CancellationToken cancellationToken);
    }
}
