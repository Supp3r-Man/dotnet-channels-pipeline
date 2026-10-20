namespace ChannelsPipeline.Tests;

public class BackpressureTests
{
    /// <summary>
    /// Single gated stage: the source can be at most capacity (queued) + parallelism (in workers) + 1 (the item the pump
    /// holds while blocked on the full queue) ahead. Each released item lets exactly one more through.
    /// </summary>
    [Theory(Timeout = Limits.Ms)]
    [InlineData(1, 1)] [InlineData(2, 1)] [InlineData(5, 1)] [InlineData(1, 3)] [InlineData(4, 3)] [InlineData(8, 2)]
    public async Task BoundedQueue_BlocksTheProducer_AtCapacityPlusWorkersPlusOne(int capacity, int parallelism)
    {
        var pulled = new Counter();
        var entered = new Signal();
        using var turnstile = new SemaphoreSlim(0);
        using var cts = new CancellationTokenSource();
        var sink = new Collector<int>();

        var run = Pipeline.From(Kit.Range(100_000, pulled))
            .Stage("work", async (int x, CancellationToken ct) =>
            {
                entered.Increment();
                await turnstile.WaitAsync(ct);
                return x;
            }, new StageOptions { Capacity = capacity, Parallelism = parallelism, Ordered = false })
            .Start(sink.Sink, cts.Token);

        var bound = capacity + parallelism + 1;
        await entered.AtLeastAsync(parallelism);
        await Kit.UntilAsync(() => pulled.Value >= bound);

        // The pump is now blocked on a full queue: nothing can move until a worker is released.
        Assert.Equal(bound, pulled.Value);
        var snap = run.Snapshot().Stages[0];
        Assert.Equal(capacity, snap.QueueDepth);
        Assert.Equal(parallelism, snap.BusyWorkers);
        Assert.Equal(capacity, snap.MaxQueueDepth);

        for (var k = 1; k <= 3; k++)
        {
            turnstile.Release();
            await Kit.UntilAsync(() => pulled.Value >= bound + k);
            Assert.Equal(bound + k, pulled.Value);
        }

        cts.Cancel();
        var report = await run.Completion;
        Assert.Equal(RunOutcome.Cancelled, report.Outcome);
        Assertions.AssertBalanced(report);
        Assert.True(report.Accepted <= bound + 3 + 1);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ThreeStages_AndASlowSink_HoldAtMostTheSumOfAllBounds()
    {
        var pulled = new Counter();
        var sinkEntered = new Signal();
        var gate = new Gate();
        using var cts2 = new CancellationTokenSource();
        var options = new PipelineOptions { SinkOptions = new StageOptions { Capacity = 2, Parallelism = 1 } };
        var run2 = Pipeline.From(Kit.Range(100_000, pulled), options)
            .Stage("a", x => x, new StageOptions { Capacity = 2, Parallelism = 2, Ordered = false })
            .Stage("b", x => x, new StageOptions { Capacity = 3, Parallelism = 1 })
            .Stage("c", x => x, new StageOptions { Capacity = 1, Parallelism = 3, Ordered = false })
            .Start(async (x, ct) => { sinkEntered.Increment(); await gate.WaitAsync(ct); }, cts2.Token);

        // pump 1 + (2+2) + (3+1) + (1+3) + (sink 2+1) = 16
        const int bound = 1 + (2 + 2) + (3 + 1) + (1 + 3) + (2 + 1);
        await Kit.UntilAsync(() => pulled.Value >= bound);
        await sinkEntered.AtLeastAsync(1);
        Assert.Equal(bound, pulled.Value);
        var snap = run2.Snapshot();
        Assert.Equal(2, snap.Stages[^1].QueueDepth);
        Assert.All(snap.Stages, s => Assert.True(s.MaxQueueDepth <= s.QueueCapacity));

        cts2.Cancel();
        var report = await run2.Completion;
        Assertions.AssertBalanced(report);
        Assert.Equal(RunOutcome.Cancelled, report.Outcome);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task OrderedStage_HoldsAtMostItsReorderWindow_WhenDownstreamIsBlocked()
    {
        var pulled = new Counter();
        var gate = new Gate();
        using var cts = new CancellationTokenSource();
        var options = new PipelineOptions { SinkOptions = new StageOptions { Capacity = 1, Parallelism = 1 } };
        var run = Pipeline.From(Kit.Range(100_000, pulled), options)
            .Stage("ord", x => x, new StageOptions { Capacity = 2, Parallelism = 3, Ordered = true }) // window = 12
            .Start(async (x, ct) => await gate.WaitAsync(ct), cts.Token);

        // pump 1 + queue 2 + window 12 + sink (queue 1 + worker 1) = 17
        const int bound = 1 + 2 + 12 + 1 + 1;
        await Kit.UntilAsync(() => pulled.Value >= bound);
        Assert.Equal(bound, pulled.Value);
        var ord = run.Snapshot().Stages[0];
        Assert.True(ord.ReorderBuffered <= ord.ReorderWindow - 1);
        Assert.Equal(12, ord.ReorderWindow);

        cts.Cancel();
        var report = await run.Completion;
        Assertions.AssertBalanced(report);
        Assert.True(report.Stages[0].MaxReorderBuffered <= 12);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SlowSink_KeepsEveryQueueWithinItsCapacity_ForAWholeRun()
    {
        const int n = 3000;
        var sink = new Collector<int>();
        var options = new PipelineOptions { SinkOptions = new StageOptions { Capacity = 4, Parallelism = 2 } };
        var report = await Pipeline.From(Kit.Range(n), options)
            .Stage("a", x => x, new StageOptions { Capacity = 8, Parallelism = 4 })
            .Stage("b", x => x, new StageOptions { Capacity = 3, Parallelism = 2, Ordered = false })
            .RunAsync(async (x, ct) =>
            {
                if (x % 2 == 0) await Task.Yield();
                sink.Add(x);
            });

        Assert.Equal(n, report.Delivered);
        Assert.All(report.Stages, s =>
        {
            Assert.InRange(s.MaxQueueDepth, 0, s.QueueCapacity);
            Assert.Equal(0, s.QueueDepth);
        });
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ProducerBlockedOnAFullQueue_IsReleasedByCancellation()
    {
        var pulled = new Counter();
        var disposed = new Flag();
        var entered = new Signal();
        using var cts = new CancellationTokenSource();
        var run = Pipeline.From(Kit.Range(1_000_000, pulled, disposed))
            .Stage("stuck", async (int x, CancellationToken ct) => { entered.Increment(); await Task.Delay(Timeout.Infinite, ct); return x; },
                new StageOptions { Capacity = 2 })
            .Start((_, _) => ValueTask.CompletedTask, cts.Token);

        await entered.AtLeastAsync(1);
        await Kit.UntilAsync(() => pulled.Value >= 4);
        cts.Cancel();
        var report = await run.Completion;

        Assert.Equal(RunOutcome.Cancelled, report.Outcome);
        Assert.True(disposed.IsSet);
        Assert.Equal(4, report.Accepted);
        Assert.Equal(4, report.NotProcessed);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task QueueDepthMetric_ReachesCapacity_WhenTheConsumerIsSlower()
    {
        var gate = new Gate();
        var entered = new Signal();
        using var cts = new CancellationTokenSource();
        var pulled = new Counter();
        var run = Pipeline.From(Kit.Range(1000, pulled))
            .Stage("slow", async (int x, CancellationToken ct) => { entered.Increment(); await gate.WaitAsync(ct); return x; },
                new StageOptions { Capacity = 7 })
            .Start((_, _) => ValueTask.CompletedTask, cts.Token);
        await entered.AtLeastAsync(1);
        await Kit.UntilAsync(() => pulled.Value >= 9);
        Assert.Equal(7, run.Snapshot().Stages[0].QueueDepth);
        gate.Open();
        var report = await run.Completion;
        Assert.Equal(7, report.Stages[0].MaxQueueDepth);
        Assert.Equal(1000, report.Delivered);
    }
}
