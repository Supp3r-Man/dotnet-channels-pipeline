using System.Globalization;
using System.Runtime.CompilerServices;

namespace ChannelsPipeline.Demo;

/// <summary>Synthetic, deterministic records. No real data anywhere.</summary>
internal sealed record RawRecord(long Id, string Line, byte[] Payload);
internal sealed record Parsed(long Id, string Customer, decimal Amount, byte[] Payload);
internal sealed record Enriched(long Id, string Customer, decimal Amount, string Region, byte[] Payload);
internal sealed record Scored(long Id, string Customer, decimal Amount, string Region, double Score, byte[] Payload);

internal static class Synthetic
{
    private static readonly string[] Regions = ["north", "south", "east", "west", "central"];

    public static async IAsyncEnumerable<RawRecord> Records(int count, [EnumeratorCancellation] CancellationToken ct = default)
    {
        for (long id = 0; id < count; id++)
        {
            ct.ThrowIfCancellationRequested();
            var line = string.Create(CultureInfo.InvariantCulture, $"{id},cust-{id % 1000:D4},{id * 37 % 10000 / 100.0:F2}");
            yield return new RawRecord(id, line, new byte[1024]);   // 1 KB payload makes resident items visible in the heap
            if (id % 4096 == 4095) await Task.Yield();
        }

        await Task.CompletedTask;
    }

    public static Parsed Parse(RawRecord r)
    {
        var parts = r.Line.Split(',');
        return new Parsed(r.Id, parts[1], decimal.Parse(parts[2], CultureInfo.InvariantCulture), r.Payload);
    }

    public static Enriched Enrich(Parsed p) =>
        new(p.Id, p.Customer, p.Amount, Regions[(int)(p.Id % Regions.Length)], p.Payload);

    public static Scored Score(Enriched e) =>
        new(e.Id, e.Customer, e.Amount, e.Region, Math.Round((double)e.Amount % 97 / 97.0, 4), e.Payload);

    /// <summary>Deterministic pseudo-random number in [0,1) from a label and an id, independent of scheduling.</summary>
    public static double Chance(string label, long id)
    {
        ulong h = 1469598103934665603UL;
        foreach (var c in label) { h ^= c; h *= 1099511628211UL; }
        h ^= (ulong)id; h *= 0x9E3779B97F4A7C15UL; h ^= h >> 32; h *= 0xBF58476D1CE4E5B9UL; h ^= h >> 29;
        return (h >> 11) / (double)(1UL << 53);
    }
}
