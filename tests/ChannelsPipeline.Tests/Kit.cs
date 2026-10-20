using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Time.Testing;

namespace ChannelsPipeline.Tests;

/// <summary>An async gate: waiters block until Open(). Never uses real time.</summary>
public sealed class Gate
{
    private readonly TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public void Open() => _tcs.TrySetResult();
    public bool IsOpen => _tcs.Task.IsCompleted;
    public Task WaitAsync() => _tcs.Task;
    public async ValueTask WaitAsync(CancellationToken ct) => await _tcs.Task.WaitAsync(ct);
}

/// <summary>A counter that can be awaited: <c>await signal.AtLeastAsync(n)</c> completes once the count reaches n.</summary>
public sealed class Signal
{
    private readonly object _lock = new();
    private readonly List<(int Target, TaskCompletionSource Tcs)> _waiters = new();
    private int _count;

    public int Count { get { lock (_lock) return _count; } }

    public int Increment()
    {
        List<TaskCompletionSource>? ready = null;
        int value;
        lock (_lock)
        {
            value = ++_count;
            for (var i = _waiters.Count - 1; i >= 0; i--)
            {
                if (_waiters[i].Target > value) continue;
                (ready ??= new()).Add(_waiters[i].Tcs);
                _waiters.RemoveAt(i);
            }
        }

        if (ready is not null) foreach (var t in ready) t.TrySetResult();
        return value;
    }

    public Task AtLeastAsync(int target)
    {
        lock (_lock)
        {
            if (_count >= target) return Task.CompletedTask;
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Add((target, tcs));
            return tcs.Task;
        }
    }
}

/// <summary>A TimeProvider backed by FakeTimeProvider that counts timer creations, so tests can wait until a timer exists before advancing.</summary>
public sealed class TestClock : TimeProvider
{
    public FakeTimeProvider Fake { get; } = new(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));
    public Signal TimersCreated { get; } = new();
    public Signal TimersDisposed { get; } = new();
    public List<TimeSpan> DueTimes { get; } = new();

    public override DateTimeOffset GetUtcNow() => Fake.GetUtcNow();
    public override long GetTimestamp() => Fake.GetTimestamp();
    public override long TimestampFrequency => Fake.TimestampFrequency;

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = new CountingTimer(Fake.CreateTimer(callback, state, dueTime, period), TimersDisposed);
        lock (DueTimes) DueTimes.Add(dueTime);
        TimersCreated.Increment();
        return timer;
    }

    private sealed class CountingTimer(ITimer inner, Signal disposed) : ITimer
    {
        private int _disposed;
        public bool Change(TimeSpan dueTime, TimeSpan period) => inner.Change(dueTime, period);
        public void Dispose() { if (Interlocked.Exchange(ref _disposed, 1) == 0) disposed.Increment(); inner.Dispose(); }
        public ValueTask DisposeAsync() { Dispose(); return ValueTask.CompletedTask; }
    }

    public void Advance(TimeSpan by) => Fake.Advance(by);
}

public static class Kit
{
    /// <summary>Yield to the scheduler until the condition holds. Bounded by the test's own timeout; no real-time delay.</summary>
    public static async Task UntilAsync(Func<bool> condition)
    {
        while (!condition()) await Task.Yield();
    }

    public static async IAsyncEnumerable<int> Range(int count, Counter? pulled = null, Flag? disposed = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        try
        {
            for (var i = 0; i < count; i++)
            {
                ct.ThrowIfCancellationRequested();
                pulled?.Increment();
                yield return i;
            }
        }
        finally
        {
            disposed?.Set();
        }

        await Task.CompletedTask;
    }

    /// <summary>Source that yields count items then throws.</summary>
    public static async IAsyncEnumerable<int> ThrowingSource(int count, Exception error, Flag? disposed = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        try
        {
            for (var i = 0; i < count; i++) yield return i;
            await Task.Yield();
            throw error;
        }
        finally
        {
            disposed?.Set();
        }
    }

    /// <summary>Source that yields items from a channel until completed; used to control arrival times.</summary>
    public static async IAsyncEnumerable<T> FromChannel<T>(System.Threading.Channels.ChannelReader<T> reader, Flag? disposed = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        try
        {
            await foreach (var x in reader.ReadAllAsync(ct)) yield return x;
        }
        finally
        {
            disposed?.Set();
        }
    }

    public static async Task<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> source, CancellationToken ct = default)
    {
        var list = new List<T>();
        await foreach (var x in source.WithCancellation(ct)) list.Add(x);
        return list;
    }

    /// <summary>Deterministic hash in [0,1) of integers, independent of thread scheduling.</summary>
    public static double Hash01(params long[] parts)
    {
        ulong h = 1469598103934665603UL;
        foreach (var p in parts)
        {
            h ^= (ulong)p;
            h *= 1099511628211UL;
            h ^= h >> 29;
            h *= 0xBF58476D1CE4E5B9UL;
            h ^= h >> 32;
        }

        return (h >> 11) / (double)(1UL << 53);
    }

    /// <summary>Yields to the scheduler a random number of times, so completion order across workers is shuffled without real time.</summary>
    public static async ValueTask RandomYields(Random rng, object gate, int max)
    {
        int n;
        lock (gate) n = rng.Next(max + 1);
        for (var i = 0; i < n; i++) await Task.Yield();
    }
}

public sealed class Counter
{
    private int _n;
    public int Value => Volatile.Read(ref _n);
    public int Increment() => Interlocked.Increment(ref _n);
    public int Decrement() => Interlocked.Decrement(ref _n);
}

public sealed class Flag
{
    private int _set;
    public bool IsSet => Volatile.Read(ref _set) == 1;
    public void Set() => Volatile.Write(ref _set, 1);
}

/// <summary>Thread-safe collector for sink output.</summary>
public sealed class Collector<T>
{
    private readonly object _lock = new();
    private readonly List<T> _items = new();
    public void Add(T item) { lock (_lock) _items.Add(item); }
    public List<T> Items { get { lock (_lock) return _items.ToList(); } }
    public int Count { get { lock (_lock) return _items.Count; } }
    public ValueTask Sink(T item, CancellationToken _) { Add(item); return ValueTask.CompletedTask; }
}

public static class Assertions
{
    public static void AssertBalanced(RunReport r)
    {
        Assert.True(r.IsBalanced, $"not balanced: accepted={r.Accepted} delivered={r.Delivered} dead={r.DeadLettered} notProcessed={r.NotProcessed}");
        Assert.Equal(r.NotProcessedSequences.Count, r.NotProcessedSequences.Distinct().Count());
        Assert.All(r.NotProcessedSequences, s => Assert.InRange(s, 0, r.Accepted - 1));
    }

    public static void AssertAllInputsCompleted(RunReport r) => Assert.All(r.Stages, s => Assert.True(s.InputCompleted, $"input of '{s.Name}' not completed"));
}
