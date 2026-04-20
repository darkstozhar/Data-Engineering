using System.Text.Json.Serialization;

namespace BlazorApp.Models;

public class Trip
{
    [JsonPropertyName("trip_id")]
    public string TripId { get; set; } = string.Empty;

    [JsonPropertyName("start_time")]
    public string StartTime { get; set; } = string.Empty;

    [JsonPropertyName("end_time")]
    public string EndTime { get; set; } = string.Empty;

    [JsonPropertyName("bikeid")]
    public string BikeId { get; set; } = string.Empty;

    [JsonPropertyName("tripduration")]
    public string TripDuration { get; set; } = string.Empty;

    [JsonPropertyName("from_station_id")]
    public string FromStationId { get; set; } = string.Empty;

    [JsonPropertyName("from_station_name")]
    public string FromStationName { get; set; } = string.Empty;

    [JsonPropertyName("to_station_id")]
    public string ToStationId { get; set; } = string.Empty;

    [JsonPropertyName("to_station_name")]
    public string ToStationName { get; set; } = string.Empty;

    [JsonPropertyName("usertype")]
    public string UserType { get; set; } = string.Empty;

    [JsonPropertyName("gender")]
    public string Gender { get; set; } = string.Empty;

    [JsonPropertyName("birthyear")]
    public string BirthYear { get; set; } = string.Empty;
}
