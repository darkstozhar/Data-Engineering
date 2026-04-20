using System.Text.Json;
using Confluent.Kafka;
using BlazorApp.Models;
using Microsoft.Extensions.Configuration;

namespace BlazorApp.Services;

public class KafkaService : IKafkaService, IDisposable
{
    private readonly ProducerConfig _producerConfig;
    private readonly ConsumerConfig _consumerConfig;
    private readonly string _topic;
    private readonly IProducer<Null, string> _producer;

    public KafkaService(IConfiguration configuration)
    {
        var bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9091";
        _topic = configuration["Kafka:Topic"] ?? "Topic1";
        var groupId = configuration["Kafka:GroupId"] ?? "lab3-group";

        _producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            MessageSendMaxRetries = 5,
            LingerMs = 5,
            SocketTimeoutMs = 10000
        };

        _consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            SessionTimeoutMs = 30000,
            HeartbeatIntervalMs = 10000,
            SocketTimeoutMs = 10000
        };

        _producer = new ProducerBuilder<Null, string>(_producerConfig).Build();
    }

    public async Task ProduceAsync(Trip trip)
    {
        await ProduceAsync(_topic, trip);
    }

    public async Task ProduceAsync<T>(string topic, T data)
    {
        var json = JsonSerializer.Serialize(data);
        await _producer.ProduceAsync(topic, new Message<Null, string> { Value = json });
    }

    /// <summary>
    /// Consumes raw Trip messages from the source topic (earliest offset for analytics).
    /// Used by the background KafkaProcessingService.
    /// </summary>
    public void StartConsuming(Action<Trip> onMessageReceived, CancellationToken cancellationToken)
    {
        Task.Run(() =>
        {
            var config = new ConsumerConfig(_consumerConfig)
            {
                GroupId = $"{_consumerConfig.GroupId}-background-analytics",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(_topic);
            Console.WriteLine($"[KafkaProcessingService] Subscribed to '{_topic}' for analytics.");

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));
                        if (consumeResult == null) continue;

                        var data = JsonSerializer.Deserialize<Trip>(
                            consumeResult.Message.Value,
                            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                        if (data != null)
                        {
                            onMessageReceived(data);
                        }
                        else
                        {
                            Console.WriteLine($"[KafkaProcessingService] Deserialized Trip was null. Raw: {consumeResult.Message.Value[..Math.Min(100, consumeResult.Message.Value.Length)]}");
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"[KafkaProcessingService] ConsumeException: {e.Error.Reason}");
                    }
                    catch (JsonException ex)
                    {
                        Console.WriteLine($"[KafkaProcessingService] JSON parse error: {ex.Message}");
                    }
                    catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                    {
                        Console.WriteLine($"[KafkaProcessingService] Unexpected error: {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                consumer.Close();
                Console.WriteLine("[KafkaProcessingService] Consumer closed.");
            }
        }, cancellationToken);
    }

    /// <summary>
    /// Subscribes to a single typed topic. Uses a unique group so the UI always reads
    /// from the latest messages in aggregation result topics.
    /// </summary>
    public void Subscribe<T>(string topic, Action<T> onMessageReceived, CancellationToken cancellationToken)
    {
        Task.Run(() =>
        {
            Console.WriteLine($"[KafkaService] Starting subscription for topic: {topic}");
            var config = new ConsumerConfig(_consumerConfig)
            {
                // Unique group = reads from earliest each time (gets all accumulated results)
                GroupId = $"{_consumerConfig.GroupId}-ui-{topic}",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(topic);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));
                        if (consumeResult == null) continue;

                        var rawJson = consumeResult.Message.Value;
                        Console.WriteLine($"[KafkaService] Received from {topic}: {rawJson[..Math.Min(100, rawJson.Length)]}");

                        var data = JsonSerializer.Deserialize<T>(rawJson,
                            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                        if (data != null)
                            onMessageReceived(data);
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"[KafkaService] ConsumeException on {topic}: {e.Error.Reason}");
                    }
                    catch (JsonException ex)
                    {
                        Console.WriteLine($"[KafkaService] JSON error on {topic}: {ex.Message}");
                    }
                    catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                    {
                        Console.WriteLine($"[KafkaService] Error on {topic}: {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                consumer.Close();
            }
        }, cancellationToken);
    }

    /// <summary>
    /// Subscribes to multiple topics at once. Used by the Dashboard page.
    /// </summary>
    public void Subscribe(IEnumerable<string> topics, Action<string, string> onMessageReceived, CancellationToken cancellationToken)
    {
        var topicList = topics.ToList();
        Task.Run(() =>
        {
            Console.WriteLine($"[KafkaService] Starting multi-topic subscription for: {string.Join(", ", topicList)}");
            var config = new ConsumerConfig(_consumerConfig)
            {
                GroupId = $"{_consumerConfig.GroupId}-ui-dashboard-{Guid.NewGuid().ToString("N")[..6]}",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(topicList);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));
                        if (consumeResult == null) continue;

                        Console.WriteLine($"[KafkaService] Dashboard received from '{consumeResult.Topic}'");
                        onMessageReceived(consumeResult.Topic, consumeResult.Message.Value);
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"[KafkaService] Dashboard ConsumeException: {e.Error.Reason}");
                    }
                    catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                    {
                        Console.WriteLine($"[KafkaService] Dashboard error: {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                consumer.Close();
            }
        }, cancellationToken);
    }

    public void Dispose()
    {
        _producer?.Flush(TimeSpan.FromSeconds(10));
        _producer?.Dispose();
    }
}
