using System.Collections.Concurrent;
using System.Globalization;
using BlazorApp.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;

namespace BlazorApp.Services;

public class KafkaProcessingService : BackgroundService
{
    private readonly IKafkaService _kafkaService;
    private readonly IConfiguration _configuration;
    
    // State storage (Date -> Aggregation)
    private readonly ConcurrentDictionary<string, (double Total, int Count)> _avgDurations = new();
    private readonly ConcurrentDictionary<string, int> _tripCounts = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, int>> _startStationHits = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, int>> _totalStationHits = new();

    private readonly string _topicAvg;
    private readonly string _topicCount;
    private readonly string _topicPopular;
    private readonly string _topicTop3;

    public KafkaProcessingService(IKafkaService kafkaService, IConfiguration configuration)
    {
        _kafkaService = kafkaService;
        _configuration = configuration;

        _topicAvg = _configuration["Kafka:TopicAvgDuration"] ?? "AvgDurationByDay";
        _topicCount = _configuration["Kafka:TopicTripCount"] ?? "TripCountByDay";
        _topicPopular = _configuration["Kafka:TopicPopularStart"] ?? "PopularStartStationByDay";
        _topicTop3 = _configuration["Kafka:TopicTop3Stations"] ?? "Top3StationsByDay";
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _kafkaService.StartConsuming(async trip =>
        {
            await ProcessTripAsync(trip);
        }, stoppingToken);

        return Task.CompletedTask;
    }

    private async Task ProcessTripAsync(Trip trip)
    {
        if (!DateTime.TryParseExact(trip.StartTime, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out var startTime))
        {
            return;
        }

        var dateKey = startTime.ToString("yyyy-MM-dd");
        var duration = ParseDuration(trip.TripDuration);

        // a. Average duration
        var avg = _avgDurations.AddOrUpdate(dateKey, (duration, 1), (key, old) => (old.Total + duration, old.Count + 1));
        await _kafkaService.ProduceAsync(_topicAvg, new DailyAverageDuration { Date = dateKey, AverageDuration = avg.Total / avg.Count });

        // b. Trip count
        var count = _tripCounts.AddOrUpdate(dateKey, 1, (key, old) => old + 1);
        await _kafkaService.ProduceAsync(_topicCount, new DailyTripCount { Date = dateKey, Count = count });

        // c. Most popular start station
        var startHits = _startStationHits.GetOrAdd(dateKey, _ => new ConcurrentDictionary<string, int>());
        startHits.AddOrUpdate(trip.FromStationName, 1, (key, old) => old + 1);
        var topStart = startHits.OrderByDescending(x => x.Value).First();
        await _kafkaService.ProduceAsync(_topicPopular, new DailyPopularStartStation 
        { 
            Date = dateKey, 
            StationName = topStart.Key, 
            Count = topStart.Value 
        });

        // d. Top 3 stations (start + end)
        var totalHits = _totalStationHits.GetOrAdd(dateKey, _ => new ConcurrentDictionary<string, int>());
        totalHits.AddOrUpdate(trip.FromStationName, 1, (key, old) => old + 1);
        totalHits.AddOrUpdate(trip.ToStationName, 1, (key, old) => old + 1);
        // Fix: Use ToArray() to get a thread-safe snapshot before LINQ operations
        var top3 = totalHits.ToArray()
            .OrderByDescending(x => x.Value)
            .Take(3)
            .Select(x => new StationRanking
            {
                StationName = x.Key,
                TotalVisits = x.Value
            }).ToList();
        
        await _kafkaService.ProduceAsync(_topicTop3, new DailyTopStations 
        { 
            Date = dateKey, 
            TopStations = top3 
        });
    }

    private double ParseDuration(string raw)
    {
        if (string.IsNullOrEmpty(raw)) return 0;
        // Remove quotes and commas (e.g. "2,350.0" -> 2350.0)
        var clean = raw.Replace("\"", "").Replace(",", "");
        if (double.TryParse(clean, NumberStyles.Any, CultureInfo.InvariantCulture, out var result))
        {
            return result;
        }
        return 0;
    }
}
