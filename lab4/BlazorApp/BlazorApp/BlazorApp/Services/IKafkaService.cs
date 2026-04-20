using BlazorApp.Models;

namespace BlazorApp.Services;

public interface IKafkaService
{
    Task ProduceAsync(Trip trip);
    Task ProduceAsync<T>(string topic, T data);
    void StartConsuming(Action<Trip> onMessageReceived, CancellationToken cancellationToken);
    void Subscribe<T>(string topic, Action<T> onMessageReceived, CancellationToken cancellationToken);
    void Subscribe(IEnumerable<string> topics, Action<string, string> onMessageReceived, CancellationToken cancellationToken);
}
