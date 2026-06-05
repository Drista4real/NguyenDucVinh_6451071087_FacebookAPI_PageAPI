using Page_API.Models;

namespace Page_API.Services
{
    public interface IFacebookCommandHandler
    {
        Task HandleAsync(ReplyCommand command, int retryCount, CancellationToken cancellationToken);
    }
}
