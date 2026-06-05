using CoreService.Models;

namespace CoreService.Services;

public interface IEventAnalyzer
{
    Task<AnalysisResult> AnalyzeAsync(
        NormalizedFacebookEvent facebookEvent,
        CancellationToken cancellationToken);
}
