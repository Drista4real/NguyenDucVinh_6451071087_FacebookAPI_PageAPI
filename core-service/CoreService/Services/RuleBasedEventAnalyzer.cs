using CoreService.Models;

namespace CoreService.Services;

public class RuleBasedEventAnalyzer : IEventAnalyzer
{
    private static readonly string[] PriceKeywords =
    [
        "gia",
        "giá",
        "bao nhieu",
        "bao nhiêu",
        "bn",
        "price"
    ];

    private static readonly string[] ComplaintKeywords =
    [
        "khieu nai",
        "khiếu nại",
        "loi",
        "lỗi",
        "te",
        "tệ",
        "cham",
        "chậm",
        "khong hai long",
        "không hài lòng"
    ];

    private static readonly string[] SpamKeywords =
    [
        "http://",
        "https://",
        "kiem tien",
        "kiếm tiền",
        "vay tien",
        "vay tiền",
        "spam"
    ];

    private static readonly string[] OrderKeywords =
    [
        "don hang",
        "đơn hàng",
        "ma don",
        "mã đơn",
        "ship",
        "giao hang",
        "giao hàng"
    ];

    private static readonly string[] PositiveKeywords =
    [
        "cam on",
        "cảm ơn",
        "tot",
        "tốt",
        "hai long",
        "hài lòng",
        "thanks"
    ];

    public Task<AnalysisResult> AnalyzeAsync(
        NormalizedFacebookEvent facebookEvent,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var message = Normalize(facebookEvent.Message);
        var intent = "unknown";
        if (ContainsAny(message, SpamKeywords))
        {
            intent = "spam";
        }
        else if (ContainsAny(message, ComplaintKeywords))
        {
            intent = "complaint";
        }
        else if (ContainsAny(message, PriceKeywords))
        {
            intent = "ask_price";
        }
        else if (ContainsAny(message, OrderKeywords))
        {
            intent = "order_status";
        }

        var sentiment = "neutral";
        if (intent == "complaint")
        {
            sentiment = "negative";
        }
        else if (ContainsAny(message, PositiveKeywords))
        {
            sentiment = "positive";
        }

        return Task.FromResult(new AnalysisResult
        {
            Intent = intent,
            Sentiment = sentiment,
            Reason = "rule_based_fallback"
        });
    }

    private static string Normalize(string? value)
    {
        return (value ?? string.Empty).Trim().ToLowerInvariant();
    }

    private static bool ContainsAny(string value, IEnumerable<string> keywords)
    {
        return keywords.Any(value.Contains);
    }
}
