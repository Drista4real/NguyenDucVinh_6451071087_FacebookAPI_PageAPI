using RetryService;
using RetryService.Models;

var builder = WebApplication.CreateBuilder(args);
builder.Services.Configure<KafkaRetryOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.AddHostedService<Worker>();

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new
{
    status = "ok",
    service = "retry-service",
    utc = DateTimeOffset.UtcNow
}));

app.Run();
