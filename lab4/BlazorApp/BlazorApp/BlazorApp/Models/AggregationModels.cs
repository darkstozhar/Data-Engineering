namespace BlazorApp.Models;

public class DailyAverageDuration
{
    public string Date { get; set; } = string.Empty;
    public double AverageDuration { get; set; }
}

public class DailyTripCount
{
    public string Date { get; set; } = string.Empty;
    public int Count { get; set; }
}

public class DailyPopularStartStation
{
    public string Date { get; set; } = string.Empty;
    public string StationName { get; set; } = string.Empty;
    public int Count { get; set; }
}

public class DailyTopStations
{
    public string Date { get; set; } = string.Empty;
    public List<StationRanking> TopStations { get; set; } = new();
}

public class StationRanking
{
    public string StationName { get; set; } = string.Empty;
    public int TotalVisits { get; set; }
}
