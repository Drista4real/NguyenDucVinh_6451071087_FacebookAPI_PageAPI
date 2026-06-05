namespace CoreService.Services;

public interface IBlacklistStore
{
    Task<bool> IsBlacklistedAsync(string userId, CancellationToken cancellationToken);
    Task<int> IncrementViolationAsync(string userId, string reason, CancellationToken cancellationToken);
}
