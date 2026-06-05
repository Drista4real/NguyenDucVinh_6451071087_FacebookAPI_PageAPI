using CoreService.Models;

namespace CoreService.Services;

public class AutomationRuleEngine
{
    private readonly IBlacklistStore _blacklistStore;
    private readonly BlacklistOptions _blacklistOptions;
    private readonly ILogger<AutomationRuleEngine> _logger;

    public AutomationRuleEngine(
        IBlacklistStore blacklistStore,
        Microsoft.Extensions.Options.IOptions<BlacklistOptions> blacklistOptions,
        ILogger<AutomationRuleEngine> logger)
    {
        _blacklistStore = blacklistStore;
        _blacklistOptions = blacklistOptions.Value;
        _logger = logger;
    }

    public async Task<ReplyCommand?> BuildCommandAsync(
        NormalizedFacebookEvent facebookEvent,
        AnalysisResult analysis,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(facebookEvent.PageId)
            || string.IsNullOrWhiteSpace(facebookEvent.CommentId))
        {
            _logger.LogWarning(
                "Cannot build command because target is missing. EventId={EventId}",
                facebookEvent.EventId);
            return null;
        }

        var action = "reply";
        var replyText = "Cam on ban da quan tam, shop se phan hoi som.";

        if (!string.IsNullOrWhiteSpace(facebookEvent.ActorId)
            && await _blacklistStore.IsBlacklistedAsync(facebookEvent.ActorId, cancellationToken))
        {
            action = "hide";
            replyText = string.Empty;
        }
        else if (string.Equals(analysis.Intent, "spam", StringComparison.OrdinalIgnoreCase))
        {
            action = "hide";
            replyText = string.Empty;

            if (!string.IsNullOrWhiteSpace(facebookEvent.ActorId))
            {
                var violations = await _blacklistStore.IncrementViolationAsync(
                    facebookEvent.ActorId,
                    "spam",
                    cancellationToken);

                if (violations >= _blacklistOptions.MaxViolationsBeforeBlacklist)
                {
                    _logger.LogInformation(
                        "User reached blacklist threshold. UserId={UserId} Violations={Violations}",
                        facebookEvent.ActorId,
                        violations);
                }
            }
        }
        else if (string.Equals(analysis.Intent, "complaint", StringComparison.OrdinalIgnoreCase)
                 || string.Equals(analysis.Sentiment, "negative", StringComparison.OrdinalIgnoreCase))
        {
            action = "manual_review";
            replyText = "Shop da ghi nhan phan anh cua ban va se kiem tra ngay.";
        }
        else if (string.Equals(analysis.Intent, "ask_price", StringComparison.OrdinalIgnoreCase))
        {
            replyText = "Da shop da gui thong tin chi tiet qua inbox.";
        }
        else if (string.Equals(analysis.Intent, "order_status", StringComparison.OrdinalIgnoreCase))
        {
            replyText = "Ban vui long inbox ma don hang de shop kiem tra nhanh hon.";
        }

        return new ReplyCommand
        {
            CommandId = $"cmd_{Guid.NewGuid():N}",
            EventId = facebookEvent.EventId,
            Action = action,
            Target = new ReplyTarget
            {
                PageId = facebookEvent.PageId,
                CommentId = facebookEvent.CommentId
            },
            ReplyText = replyText,
            Intent = analysis.Intent,
            Sentiment = analysis.Sentiment,
            CreatedAt = DateTimeOffset.UtcNow
        };
    }
}
