using CoreService.Models;
using CoreService.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.Configure<AnalysisOptions>(builder.Configuration.GetSection("Analysis"));
builder.Services.Configure<BlacklistOptions>(builder.Configuration.GetSection("Blacklist"));

builder.Services.AddSingleton<IEventAnalyzer, RuleBasedEventAnalyzer>();
builder.Services.AddSingleton<IBlacklistStore, PostgresBlacklistStore>();
builder.Services.AddSingleton<AutomationRuleEngine>();
builder.Services.AddHostedService<CoreWorker>();

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new
{
    status = "ok",
    service = "core-service",
    utc = DateTimeOffset.UtcNow
}));

app.Run();
