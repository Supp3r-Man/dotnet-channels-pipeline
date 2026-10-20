using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace ChannelsPipeline.Tests;

public class BatchTests
{
    /// <summary>Channel-backed source that signals each time the consumer comes back for the next item, i.e. the previous item was taken.</summary>
    private static async IAsyncEnumerable<int> Source(ChannelReader<int> reader, Signal resumed, Flag? disposed = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        try
        {
            await foreach (var x in reader.ReadAllAsync(ct))
            {
                yield return x;
                resumed.Increment();
            }
        }
        finally
        {
            disposed?.Set();
        }
    }

    [Theory(Timeout = Limits.Ms)]
    [InlineData(0, 3)] [InlineData(1, 3)] [InlineData(2, 3)] [InlineData(3, 3)] [InlineData(7, 3)] [InlineData(9, 3)]
    [InlineData(10, 1)] [InlineData(10, 10)] [InlineData(10, 11)] [InlineData(1000, 64)]
    public async Task FlushBySize_AndTheRemainderAtTheEnd(int count, int size)
    {
        var batches = await Kit.Range(count).Batch(size, Timeout.InfiniteTimeSpan).ToListAsync();
        Assert.Equal(Enumerable.Range(0, count), batches.SelectMany(b => b));
        Assert.All(batches.SkipLast(1), b => Assert.Equal(size, b.Count));
        Assert.Equal((count + size - 1) / size, batches.Count);
        Assert.All(batches, b => Assert.NotEmpty(b));
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SizeOnlyBatching_NeverCreatesATimer()
    {
        var clock = new TestClock();
        // maxWait is infinite so no timer may ever be armed
        await Kit.Range(50).Batch(5, Timeout.InfiniteTimeSpan, clock).ToListAsync();
        Assert.Equal(0, clock.TimersCreated.Count);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task FlushByTime_EmitsThePartialBatch_WhenMaxWaitElapsesOnTheFakeClock()
    {
        var clock = new TestClock();
        var channel = Channel.CreateUnbounded<int>();
        var resumed = new Signal();
        await using var batches = Source(channel.Reader, resumed).Batch(10, TimeSpan.FromSeconds(5), clock).GetAsyncEnumerator();

        var next = batches.MoveNextAsync().AsTask();
        channel.Writer.TryWrite(1);
        channel.Writer.TryWrite(2);
        await resumed.AtLeastAsync(2);                 // both items are in the batch, timer armed
        Assert.Equal(1, clock.TimersCreated.Count);

        clock.Advance(TimeSpan.FromSeconds(4.9));
        Assert.False(next.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(0.1));
        Assert.True(await next);
        Assert.Equal(new[] { 1, 2 }, batches.Current);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task TheTimerRestartsWithTheFirstItemOfTheNextBatch()
    {
        var clock = new TestClock();
        var channel = Channel.CreateUnbounded<int>();
        var resumed = new Signal();
        await using var batches = Source(channel.Reader, resumed).Batch(10, TimeSpan.FromSeconds(5), clock).GetAsyncEnumerator();

        var next = batches.MoveNextAsync().AsTask();
        channel.Writer.TryWrite(1);
        await resumed.AtLeastAsync(1);
        clock.Advance(TimeSpan.FromSeconds(5));
        Assert.True(await next);
        Assert.Equal(new[] { 1 }, batches.Current);

        next = batches.MoveNextAsync().AsTask();
        clock.Advance(TimeSpan.FromHours(1));          // no items: nothing to flush, no empty batch
        Assert.False(next.IsCompleted);
        channel.Writer.TryWrite(2);
        await resumed.AtLeastAsync(2);
        Assert.Equal(2, clock.TimersCreated.Count);    // a fresh timer for the new batch
        clock.Advance(TimeSpan.FromSeconds(5));
        Assert.True(await next);
        Assert.Equal(new[] { 2 }, batches.Current);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SizeFlush_DisarmsTheTimer_SoNoSpuriousEmptyBatchFollows()
    {
        var clock = new TestClock();
        var channel = Channel.CreateUnbounded<int>();
        var resumed = new Signal();
        var results = new List<IReadOnlyList<int>>();
        var consume = Task.Run(async () =>
        {
            await foreach (var b in Source(channel.Reader, resumed).Batch(3, TimeSpan.FromSeconds(5), clock)) results.Add(b);
        });

        for (var i = 0; i < 3; i++) channel.Writer.TryWrite(i);
        await resumed.AtLeastAsync(3);
        clock.Advance(TimeSpan.FromHours(2));
        channel.Writer.Complete();
        await consume;

        Assert.Single(results);
        Assert.Equal(new[] { 0, 1, 2 }, results[0]);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task MixedSizeAndTimeFlushes_PreserveOrderAndLoseNothing()
    {
        var clock = new TestClock();
        var channel = Channel.CreateUnbounded<int>();
        var resumed = new Signal();
        var results = new List<IReadOnlyList<int>>();
        var consume = Task.Run(async () =>
        {
            await foreach (var b in Source(channel.Reader, resumed).Batch(4, TimeSpan.FromSeconds(1), clock)) { lock (results) results.Add(b); }
        });

        // 4 (size flush), 2 (time flush), 5 (size flush of 4 + remainder of 1 flushed at the end)
        for (var i = 0; i < 4; i++) channel.Writer.TryWrite(i);
        await resumed.AtLeastAsync(4);
        for (var i = 4; i < 6; i++) channel.Writer.TryWrite(i);
        await resumed.AtLeastAsync(6);
        clock.Advance(TimeSpan.FromSeconds(1));
        await Kit.UntilAsync(() => { lock (results) return results.Count == 2; });
        for (var i = 6; i < 11; i++) channel.Writer.TryWrite(i);
        channel.Writer.Complete();
        await consume;

        Assert.Equal(new[] { 4, 2, 4, 1 }, results.Select(r => r.Count));
        Assert.Equal(Enumerable.Range(0, 11), results.SelectMany(r => r));
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SourceError_Propagates_AndTheSourceIsDisposed()
    {
        var disposed = new Flag();
        var boom = new InvalidOperationException("source");
        var seen = new List<IReadOnlyList<int>>();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var b in Kit.ThrowingSource(5, boom, disposed).Batch(3, Timeout.InfiniteTimeSpan)) seen.Add(b);
        });
        Assert.Same(boom, ex);
        Assert.Single(seen);                    // [0,1,2]; the partial [3,4] is dropped when the source fails
        Assert.True(disposed.IsSet);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ConsumerLeavingEarly_DisposesTheSource_AndCancelsThePendingRead()
    {
        var clock = new TestClock();
        var channel = Channel.CreateUnbounded<int>();
        var resumed = new Signal();
        var disposed = new Flag();
        await using (var batches = Source(channel.Reader, resumed, disposed).Batch(2, TimeSpan.FromSeconds(1), clock).GetAsyncEnumerator())
        {
            channel.Writer.TryWrite(1);
            channel.Writer.TryWrite(2);
            Assert.True(await batches.MoveNextAsync());
            Assert.Equal(new[] { 1, 2 }, batches.Current);
        }   // leaves while a read is outstanding on an open channel

        Assert.True(disposed.IsSet);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task CancellationToken_StopsBatching()
    {
        using var cts = new CancellationTokenSource();
        var channel = Channel.CreateUnbounded<int>();
        var resumed = new Signal();
        var task = Task.Run(async () =>
        {
            await foreach (var _ in Source(channel.Reader, resumed).Batch(10, Timeout.InfiniteTimeSpan).WithCancellation(cts.Token)) { }
        });
        channel.Writer.TryWrite(1);
        await resumed.AtLeastAsync(1);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
    }

    [Theory(Timeout = Limits.Ms)]
    [InlineData(0)] [InlineData(-1)]
    public void InvalidSize_IsRejected(int size) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => Kit.Range(1).Batch(size, TimeSpan.FromSeconds(1)));

    [Theory(Timeout = Limits.Ms)]
    [InlineData(0)] [InlineData(-5)]
    public void NonPositiveMaxWait_IsRejected(int ms) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => Kit.Range(1).Batch(2, TimeSpan.FromMilliseconds(ms)));

    [Fact(Timeout = Limits.Ms)]
    public void NullSource_IsRejected() =>
        Assert.Throws<ArgumentNullException>(() => ((IAsyncEnumerable<int>)null!).Batch(2, TimeSpan.FromSeconds(1)));

    [Fact(Timeout = Limits.Ms)]
    public async Task BatchedSource_FeedsAPipeline()
    {
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(100).Batch(10, Timeout.InfiniteTimeSpan))
            .Stage("sum", (IReadOnlyList<int> b) => b.Sum(), new StageOptions { Parallelism = 3 })
            .RunAsync(sink.Sink);
        Assert.Equal(10, report.Delivered);
        Assert.Equal(Enumerable.Range(0, 10).Select(i => Enumerable.Range(i * 10, 10).Sum()), sink.Items);
    }
}

public class MergeTests
{
    [Fact(Timeout = Limits.Ms)]
    public async Task EveryItemFromEverySource_ArrivesExactlyOnce_AndEachSourcesOrderIsKept()
    {
        async IAsyncEnumerable<(int Src, int Seq)> Src(int id, int count, [EnumeratorCancellation] CancellationToken ct = default)
        {
            for (var i = 0; i < count; i++)
            {
                if (i % 5 == 0) await Task.Yield();
                yield return (id, i);
            }
        }

        var merged = await Fan.Merge(Src(0, 500), Src(1, 300), Src(2, 700)).ToListAsync();
        Assert.Equal(1500, merged.Count);
        foreach (var (id, count) in new[] { (0, 500), (1, 300), (2, 700) })
            Assert.Equal(Enumerable.Range(0, count), merged.Where(m => m.Src == id).Select(m => m.Seq));
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task NoSources_YieldsNothing() =>
        Assert.Empty(await Fan.Merge<int>().ToListAsync());

    [Fact(Timeout = Limits.Ms)]
    public async Task SingleSource_PassesThrough() =>
        Assert.Equal(Enumerable.Range(0, 100), await Fan.Merge(Kit.Range(100)).ToListAsync());

    [Fact(Timeout = Limits.Ms)]
    public async Task EmptySources_AreFine() =>
        Assert.Equal(new[] { 0, 1, 2 }, (await Fan.Merge(Kit.Range(0), Kit.Range(3), Kit.Range(0)).ToListAsync()).OrderBy(x => x));

    [Fact(Timeout = Limits.Ms)]
    public async Task AFailingSource_CancelsItsSiblings_AndThrowsToTheConsumer()
    {
        var siblingDisposed = new Flag();
        var siblingStarted = new Signal();
        async IAsyncEnumerable<int> Endless([EnumeratorCancellation] CancellationToken ct = default)
        {
            try
            {
                siblingStarted.Increment();
                await Task.Delay(Timeout.Infinite, ct);
                yield return 0;
            }
            finally
            {
                siblingDisposed.Set();
            }
        }

        async IAsyncEnumerable<int> Failing([EnumeratorCancellation] CancellationToken ct = default)
        {
            await siblingStarted.AtLeastAsync(1);
            await Task.Yield();
            throw new InvalidOperationException("one source failed");
            #pragma warning disable CS0162
            yield break;
            #pragma warning restore CS0162
        }

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in Fan.Merge(Endless(), Failing())) { }
        });
        Assert.Equal("one source failed", ex.Message);
        Assert.True(siblingDisposed.IsSet);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ConsumerLeavingEarly_CancelsAndDisposesAllSources()
    {
        var disposedA = new Flag();
        var disposedB = new Flag();
        async IAsyncEnumerable<int> Infinite(Flag disposed, [EnumeratorCancellation] CancellationToken ct = default)
        {
            try
            {
                var i = 0;
                while (true) { ct.ThrowIfCancellationRequested(); yield return i++; await Task.Yield(); }
            }
            finally { disposed.Set(); }
        }

        await foreach (var x in Fan.Merge(Infinite(disposedA), Infinite(disposedB)))
            if (x >= 20) break;
        Assert.True(disposedA.IsSet);
        Assert.True(disposedB.IsSet);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SlowConsumer_BackpressuresEverySource_ToCapacityPlusSourcesPlusOne()
    {
        var pulled = new Counter();
        async IAsyncEnumerable<int> Infinite([EnumeratorCancellation] CancellationToken ct = default)
        {
            while (true) { ct.ThrowIfCancellationRequested(); pulled.Increment(); yield return 0; }
        }

        await using var e = Fan.Merge(new[] { Infinite(), Infinite() }, capacity: 2).GetAsyncEnumerator();
        Assert.True(await e.MoveNextAsync());   // the consumer has taken one item and now stops reading
        await Kit.UntilAsync(() => pulled.Value >= 5);
        // 1 consumed + 2 queued + 1 held by each of the 2 blocked pumps
        Assert.Equal(5, pulled.Value);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task CancellationToken_StopsTheMergedSequence()
    {
        using var cts = new CancellationTokenSource();
        async IAsyncEnumerable<int> Infinite([EnumeratorCancellation] CancellationToken ct = default)
        {
            while (true) { ct.ThrowIfCancellationRequested(); yield return 1; await Task.Yield(); }
        }

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            var n = 0;
            await foreach (var _ in Fan.Merge(Infinite(), Infinite()).WithCancellation(cts.Token))
                if (++n == 10) cts.Cancel();
        });
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task MergedSources_FeedAPipeline_WithOrderedStage()
    {
        var sink = new Collector<int>();
        var report = await Pipeline.From(Fan.Merge(Kit.Range(100), Kit.Range(100), Kit.Range(100)))
            .Stage("inc", async (int x, CancellationToken _) => { await Task.Yield(); return x + 1; }, new StageOptions { Parallelism = 4 })
            .RunAsync(sink.Sink);
        Assert.Equal(300, report.Delivered);
        Assert.Equal(Enumerable.Range(1, 100).SelectMany(x => new[] { x, x, x }), sink.Items.OrderBy(x => x));
    }

    [Theory(Timeout = Limits.Ms)]
    [InlineData(0)] [InlineData(-1)]
    public void InvalidCapacity_IsRejected(int capacity) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => Fan.Merge(new[] { Kit.Range(1) }, capacity));
}
