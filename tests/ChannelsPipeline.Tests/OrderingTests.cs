namespace ChannelsPipeline.Tests;

public class OrderingTests
{
    /// <summary>Random per-item delays (a seeded number of scheduler yields) shuffle completion order across workers.</summary>
    [Theory(Timeout = Limits.Ms)]
    [InlineData(1, 2)] [InlineData(2, 2)] [InlineData(3, 2)]
    [InlineData(4, 4)] [InlineData(5, 4)] [InlineData(6, 4)] [InlineData(7, 4)]
    [InlineData(8, 8)] [InlineData(9, 8)] [InlineData(10, 8)] [InlineData(11, 16)] [InlineData(12, 16)]
    public async Task Ordered_PreservesInputOrder_UnderRandomDelays_AndBufferStaysWithinWindow(int seed, int parallelism)
    {
        const int n = 3000;
        var rng = new Random(seed);
        var rngLock = new object();
        var window = parallelism + seed % 5 * parallelism / 2 + (seed % 2); // varied but always >= parallelism
        var sink = new Collector<int>();

        var report = await Pipeline.From(Kit.Range(n))
            .Stage("shuffle", async (int x, CancellationToken _) =>
            {
                await Kit.RandomYields(rng, rngLock, x % 17 == 0 ? 200 : 20);
                return x * 10;
            }, new StageOptions { Capacity = 4, Parallelism = parallelism, Ordered = true, ReorderWindow = window })
            .RunAsync(sink.Sink);

        Assert.Equal(Enumerable.Range(0, n).Select(x => x * 10), sink.Items);
        var s = report.Stages[0];
        Assert.Equal(window, s.ReorderWindow);
        Assert.True(s.MaxReorderBuffered <= window, $"buffered {s.MaxReorderBuffered} > window {window}");
        Assert.True(s.MaxBusyWorkers <= parallelism);
        Assert.Equal(0, s.ReorderBuffered);
        Assertions.AssertBalanced(report);
    }

    /// <summary>Head item gated: the other items finish and wait; the buffer fills to exactly window-1 and the stage admits no more.</summary>
    [Theory(Timeout = Limits.Ms)]
    [InlineData(2, 2)] [InlineData(2, 5)] [InlineData(3, 3)] [InlineData(4, 4)] [InlineData(4, 10)] [InlineData(8, 9)]
    public async Task ReorderBuffer_FillsToExactlyWindowMinusOne_ThenStageStopsAdmittingItems(int parallelism, int window)
    {
        const int n = 60;
        var gate = new Gate();
        var started = new Signal();
        var sink = new Collector<int>();

        var run = Pipeline.From(Kit.Range(n))
            .Stage("head-gated", async (int x, CancellationToken ct) =>
            {
                started.Increment();
                if (x == 0) await gate.WaitAsync(ct);
                else await Task.Yield();
                return x;
            }, new StageOptions { Capacity = 4, Parallelism = parallelism, Ordered = true, ReorderWindow = window })
            .Start(sink.Sink);

        await started.AtLeastAsync(window);
        await Kit.UntilAsync(() => run.Snapshot().Stages[0].ReorderBuffered == window - 1);

        // Nothing can be delivered before item 0, and no item beyond the window may even start.
        Assert.Equal(0, sink.Count);
        Assert.Equal(window, started.Count);
        Assert.Equal(window - 1, run.Snapshot().Stages[0].MaxReorderBuffered);

        gate.Open();
        var report = await run.Completion;
        Assert.Equal(Enumerable.Range(0, n), sink.Items);
        Assert.InRange(report.Stages[0].MaxReorderBuffered, window - 1, window);
        Assert.Equal(n, report.Delivered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task Ordered_WithWindowEqualToParallelism_NeverBuffersMoreThanParallelismMinusOne()
    {
        var rng = new Random(99);
        var gate = new object();
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(2000))
            .Stage("s", async (int x, CancellationToken _) => { await Kit.RandomYields(rng, gate, 40); return x; },
                new StageOptions { Parallelism = 5, ReorderWindow = 5, Capacity = 2 })
            .RunAsync(sink.Sink);
        Assert.Equal(Enumerable.Range(0, 2000), sink.Items);
        Assert.True(report.Stages[0].MaxReorderBuffered <= 6);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task Ordered_WithOneWorker_NeedsNoReorderBuffer()
    {
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(100)).Stage("s", x => x).RunAsync(sink.Sink);
        Assert.Equal(0, report.Stages[0].ReorderWindow);
        Assert.Equal(0, report.Stages[0].MaxReorderBuffered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task TwoOrderedStagesInARow_KeepOrder()
    {
        var rng = new Random(5);
        var gate = new object();
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(2500))
            .Stage("a", async (int x, CancellationToken _) => { await Kit.RandomYields(rng, gate, 25); return x + 1; }, new StageOptions { Parallelism = 6, Capacity = 3 })
            .Stage("b", async (int x, CancellationToken _) => { await Kit.RandomYields(rng, gate, 25); return x * 2; }, new StageOptions { Parallelism = 3, Capacity = 5 })
            .RunAsync(sink.Sink);
        Assert.Equal(Enumerable.Range(0, 2500).Select(x => (x + 1) * 2), sink.Items);
        Assert.All(report.Stages.Take(2), s => Assert.True(s.MaxReorderBuffered <= s.ReorderWindow));
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task Ordered_WithDeadLetters_StillEmitsSurvivorsInOrder_AndDoesNotStall()
    {
        var rng = new Random(11);
        var gate = new object();
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(1400))
            .Stage("flaky", async (int x, CancellationToken _) =>
            {
                await Kit.RandomYields(rng, gate, 20);
                if (x % 7 == 0) throw new InvalidOperationException("boom " + x);
                return x;
            }, new StageOptions { Parallelism = 4, Capacity = 3, ReorderWindow = 6 })
            .RunAsync(sink.Sink);

        Assert.Equal(Enumerable.Range(0, 1400).Where(x => x % 7 != 0), sink.Items);
        Assert.Equal(200, report.DeadLettered);
        Assert.Equal(1200, report.Delivered);
        Assert.True(report.Stages[0].MaxReorderBuffered <= 6);
        Assertions.AssertBalanced(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task Ordered_DeadLetteredHead_DoesNotBlockTheItemsBehindIt()
    {
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(30))
            .Stage("s", async (int x, CancellationToken _) =>
            {
                await Task.Yield();
                if (x == 0) throw new InvalidOperationException("head fails");
                return x;
            }, new StageOptions { Parallelism = 3, ReorderWindow = 3 })
            .RunAsync(sink.Sink);
        Assert.Equal(Enumerable.Range(1, 29), sink.Items);
        Assert.Equal(1, report.DeadLettered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task Unordered_DoesNotWaitForASlowItem_AndDeliversEveryItemExactlyOnce()
    {
        var gate = new Gate();
        var sinkSignal = new Signal();
        var sink = new Collector<int>();
        var run = Pipeline.From(Kit.Range(40))
            .Stage("u", async (int x, CancellationToken ct) =>
            {
                if (x == 0) await gate.WaitAsync(ct);
                return x;
            }, new StageOptions { Parallelism = 3, Ordered = false, Capacity = 4 })
            .Start((x, ct) => { sink.Add(x); sinkSignal.Increment(); return ValueTask.CompletedTask; });

        await sinkSignal.AtLeastAsync(10);          // ten items overtook item 0
        Assert.DoesNotContain(0, sink.Items);
        gate.Open();
        var report = await run.Completion;

        var got = sink.Items;
        Assert.Equal(Enumerable.Range(0, 40), got.OrderBy(x => x));
        Assert.True(got.IndexOf(0) >= 10);
        Assert.Equal(0, report.Stages[0].MaxReorderBuffered);
        Assert.Equal(0, report.Stages[0].ReorderWindow);
    }

    [Theory(Timeout = Limits.Ms)]
    [InlineData(1, 1)] [InlineData(2, 3)] [InlineData(3, 8)] [InlineData(4, 16)]
    public async Task Unordered_RandomDelays_DeliversEveryItemExactlyOnce(int seed, int parallelism)
    {
        const int n = 4000;
        var rng = new Random(seed);
        var rngLock = new object();
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(n))
            .Stage("u", async (int x, CancellationToken _) => { await Kit.RandomYields(rng, rngLock, 30); return x; },
                new StageOptions { Parallelism = parallelism, Ordered = false, Capacity = 5 })
            .RunAsync(sink.Sink);
        var got = sink.Items;
        Assert.Equal(n, got.Count);
        Assert.Equal(Enumerable.Range(0, n), got.OrderBy(x => x));
        Assert.Equal(n, report.Delivered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SinkParallelism_RunsSinkCallsConcurrently_AndDeliversAll()
    {
        var barrier2 = new Barrier(3);
        var options = new PipelineOptions { SinkOptions = new StageOptions { Parallelism = 3, Capacity = 3 } };
        var report2 = await Pipeline.From(Kit.Range(3), options)
            .RunAsync(async (x, ct) => await Task.Run(() => barrier2.SignalAndWait(TimeSpan.FromSeconds(30)), ct));
        Assert.Equal(3, report2.Delivered);
        Assert.Equal(3, report2.Stages[^1].MaxBusyWorkers);
    }
}
