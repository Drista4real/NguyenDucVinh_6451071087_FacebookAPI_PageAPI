namespace CoreService.Models;

public class AnalysisResult
{
    public string Intent { get; set; } = "unknown";
    public string Sentiment { get; set; } = "neutral";
    public string? Reason { get; set; }

    public static AnalysisResult Fallback(string? reason = null)
    {
        return new AnalysisResult
        {
            Intent = "unknown",
            Sentiment = "neutral",
            Reason = reason
        };
    }
}
