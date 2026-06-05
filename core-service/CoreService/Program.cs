using Confluent.Kafka;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHostedService<CoreWorker>();

var app = builder.Build();
app.Run();

public class CoreWorker : BackgroundService
{
    private readonly IConfiguration _config;
    public CoreWorker(IConfiguration config) => _config = config;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _config["Kafka:BootstrapServers"],
            GroupId = _config["Kafka:GroupId"],
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var producerConfig = new ProducerConfig { BootstrapServers = _config["Kafka:BootstrapServers"] };
        using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        consumer.Subscribe(_config["Kafka:Topic"]);
        Console.WriteLine("--- CORE SERVICE ĐÃ SẴN SÀNG ---");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                var rawMessage = result.Message.Value;

                // 1. Parse JSON thực tế từ Facebook (đẩy qua Webhook Service)
                using var doc = JsonDocument.Parse(rawMessage);
                var root = doc.RootElement;

                // Trích xuất từ RawEvent -> value -> comment_id/post_id
                var rawEventValue = root.GetProperty("RawEvent").GetProperty("value");
                string postId = rawEventValue.GetProperty("post_id").GetString();
                string commentId = rawEventValue.GetProperty("comment_id").GetString();
                string message = rawEventValue.GetProperty("message").GetString();

                Console.WriteLine($"Đã nhận tin từ Post: {postId}, Comment: {message}");

                // 2. Logic xử lý (Tự động trả lời)
                var command = new
                {
                    Action = "reply",
                    Message = "Cảm ơn bạn đã quan tâm, shop sẽ inbox ngay!",
                    PostId = postId,
                    TargetCommentId = commentId
                };

                // 3. Đẩy sang Backend API
                await producer.ProduceAsync("reply_commands", new Message<string, string>
                {
                    Key = commentId, // Dùng commentId làm Key để đảm bảo thứ tự
                    Value = JsonSerializer.Serialize(command)
                });

                Console.WriteLine("Đã đẩy lệnh vào reply_commands!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Lỗi xử lý: {ex.Message}");
            }
        }
    }
}