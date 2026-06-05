namespace Page_API.Services
{
    public interface IProcessedCommandStore
    {
        Task<bool> IsProcessedAsync(string idempotencyKey, CancellationToken cancellationToken);
        Task MarkProcessedAsync(
            string idempotencyKey,
            string commandId,
            string action,
            CancellationToken cancellationToken);
    }
}
