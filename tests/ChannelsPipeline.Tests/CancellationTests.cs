using System.Collections.Concurrent;

namespace ChannelsPipeline.Tests;

public class CancellationTests
{
    [Fact(Timeout = Limits.Ms)]
    public async Task CancelMidFlight_LeavesNoStuckTasks_AndAccountsForEveryAcceptedItem()
    {
        var entered = new Signal();
        var disposed = new Flag();
        using var cts = new CancellationTokenSource();
        var sink = new Collector<int>();
        var run = Pipeline.From(Kit.Range(1_000_000, disposed: disposed))
            .Stage("a", x => x, new StageOptions { Capacity = 4, Parallelism = 2 })
            .Stage("park", async (int x, CancellationToken ct) => { entered.Increment(); await Task.Delay(Timeout.Infinite, ct); return x; },
                new StageOptions { Capacity = 4, Parallelism = 3 })
            .Start(sink.Sink, cts.Token);

        await entered.AtLeastAsync(3);
        cts.Cancel();
        var report = await run.Completion;

        Assert.True(run.Completion.IsCompletedSuccessfully);
        Assert.Equal(RunOutcome.Cancelled, report.Outcome);
        Assert.Null(report.FatalError);
        Assert.Equal(0, report.Delivered);
        Assert.Equal(report.Accepted, report.NotProcessed);
        Assert.True(disposed.IsSet);
        Assertions.AssertBalanced(report);
        Assertions.AssertAllInputsCompleted(report);
        Assert.All(report.Stages, s => Assert.Equal(0, s.BusyWorkers));
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task CancelBeforeStart_ReportsCancelled_WithNothingAccepted()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var report = await Pipeline.From(Kit.Range(100)).Stage("a", x => x).RunAsync(_ => { }, cts.Token);
        Assert.Equal(RunOutcome.Cancelled, report.Outcome);
        Assert.Equal(0, report.Accepted);
        Assert.Equal(0, report.Delivered);
        Assertions.AssertBalanced(report);
        Assertions.AssertAllInputsCompleted(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task CancelAfterCompletion_DoesNotChangeTheOutcome()
    {
        using var cts = new CancellationTokenSource();
        var report = await Pipeline.From(Kit.Range(50)).Stage("a", x => x).RunAsync(_ => { }, cts.Token);
        cts.Cancel();
        Assert.Equal(RunOutcome.Completed, report.Outcome);
        Assert.Equal(50, report.Delivered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task CancelDuringRetryBackoff_AbandonsTheItem_WithoutDeadLettering()
    {
        var clock = new TestClock();
        var attempts = new Signal();
        using var cts = new CancellationTokenSource();
        var run = Pipeline.From(new[] { 1 }, new PipelineOptions { TimeProvider = clock })
            .Stage<int>("f", (x, _) => { attempts.Increment(); throw new Exception("x"); },
                new StageOptions { Retry = RetryPolicy.Fixed(5, TimeSpan.FromHours(1)) })
            .Start((_, _) => ValueTask.CompletedTask, cts.Token);

        await attempts.AtLeastAsync(1);
        await clock.TimersCreated.AtLeastAsync(1);
        cts.Cancel();
        var report = await run.Completion;

        Assert.Equal(RunOutcome.Cancelled, report.Outcome);
        Assert.Equal(1, attempts.Count);
        Assert.Equal(0, report.DeadLettered);
        Assert.Equal(new long[] { 0 }, report.NotProcessedSequences);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task CancelWhileSinkIsBlocked_ItemIsNotCountedAsDelivered()
    {
        var entered = new Signal();
        using var cts = new CancellationTokenSource();
        var run = Pipeline.From(Kit.Range(5))
            .Start(async (x, ct) => { entered.Increment(); await Task.Delay(Timeout.Infinite, ct); }, cts.Token);
        await entered.AtLeastAsync(1);
        cts.Cancel();
        var report = await run.Completion;
        Assert.Equal(RunOutcome.Cancelled, report.Outcome);
        Assert.Equal(0, report.Delivered);
        Assert.Equal(report.Accepted, report.NotProcessed);
        Assert.Contains(0L, report.NotProcessedSequences);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task CancelWhileOrderedStageWaitsForItsGatedHead_DoesNotHang()
    {
        var started = new Signal();
        using var cts = new CancellationTokenSource();
        var run = Pipeline.From(Kit.Range(1000))
            .Stage("ord", async (int x, CancellationToken ct) =>
            {
                started.Increment();
                if (x == 0) await Task.Delay(Timeout.Infinite, ct);
                return x;
            }, new StageOptions { Parallelism = 4, ReorderWindow = 8 })
            .Start((_, _) => ValueTask.CompletedTask, cts.Token);
        await started.AtLeastAsync(8);
        cts.Cancel();
        var report = await run.Completion;
        Assert.Equal(RunOutcome.Cancelled, report.Outcome);
        Assert.Equal(0, report.Delivered);
        Assertions.AssertBalanced(report);
        Assertions.AssertAllInputsCompleted(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task CancelInsideAStageFunction_StopsTheRun()
    {
        using var cts = new CancellationTokenSource();
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(1_000_000))
            .Stage("a", (int x) => { if (x == 50) cts.Cancel(); return x; }, new StageOptions { Capacity = 4 })
            .RunAsync(sink.Sink, cts.Token);
        Assert.Equal(RunOutcome.Cancelled, report.Outcome);
        Assert.True(report.Accepted < 200);
        Assertions.AssertBalanced(report);
        Assert.Equal(sink.Count, report.Delivered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task OuterCancellation_IsNotMistakenForAnItemTimeout()
    {
        var clock = new TestClock();
        var entered = new Signal();
        using var cts = new CancellationTokenSource();
        var run = Pipeline.From(new[] { 1 }, new PipelineOptions { TimeProvider = clock })
            .Stage("f", async (int x, CancellationToken ct) => { entered.Increment(); await Task.Delay(Timeout.Infinite, ct); return x; },
                new StageOptions { ItemTimeout = TimeSpan.FromHours(1) })
            .Start((_, _) => ValueTask.CompletedTask, cts.Token);
        await entered.AtLeastAsync(1);
        cts.Cancel();
        var report = await run.Completion;
        Assert.Equal(RunOutcome.Cancelled, report.Outcome);
        Assert.Equal(0, report.DeadLettered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task CancellationDuringFailFast_KeepsTheFatalErrorAsTheOutcome()
    {
        using var cts = new CancellationTokenSource();
        var report = await Pipeline.From(Kit.Range(100), new PipelineOptions { FailureMode = FailureMode.FailFast })
            .Stage<int>("f", (x, _) => { cts.Cancel(); throw new Exception("fail then cancel"); })
            .RunAsync(_ => { }, cts.Token);
        // cancel() was called by the function before it threw: either the run saw the cancel first (Cancelled) or the failure (Faulted); never both.
        Assert.Contains(report.Outcome, new[] { RunOutcome.Cancelled, RunOutcome.Faulted });
        Assertions.AssertBalanced(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ManySmallRuns_CancelledAtVariousPoints_AllFinish()
    {
        for (var i = 0; i < 60; i++)
        {
            using var cts = new CancellationTokenSource();
            var calls = new Counter();
            var report = await Pipeline.From(Kit.Range(10_000))
                .Stage("a", (int x) => { if (calls.Increment() == i + 1) cts.Cancel(); return x; }, new StageOptions { Capacity = 3, Parallelism = 1 + i % 4 })
                .RunAsync(_ => { }, cts.Token);
            Assertions.AssertBalanced(report);
            Assertions.AssertAllInputsCompleted(report);
        }
    }
}

/// <summary>
/// Seeded random property test: random pipeline shapes, failure rates, failure modes and cancel points.
/// Invariants: the report balances (delivered + dead-lettered + not-processed = accepted), nothing is delivered twice,
/// nothing is delivered or dead-lettered that the failure oracle says should not be, completed runs match the oracle exactly,
/// all-ordered shapes deliver in ascending order, the source is disposed, and every queue was completed.
/// </summary>
public class PropertyTests
{
    private sealed record Spec(int Capacity, int Parallelism, bool Ordered, int Attempts, double FailRate, int? Window);

    [Theory(Timeout = Limits.Ms)]
    [InlineData(0)] [InlineData(1)] [InlineData(2)] [InlineData(3)] [InlineData(4)]
    [InlineData(5)] [InlineData(6)] [InlineData(7)] [InlineData(8)] [InlineData(9)]
    public async Task RandomPipelines_AlwaysAccountForEveryAcceptedItem(int block)
    {
        for (var i = 0; i < 30; i++)
        {
            var seed = block * 30 + i;
            try
            {
                await RunSeed(seed);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new InvalidOperationException($"property failed for seed {seed}: {ex.Message}", ex);
            }
        }
    }

    private static async Task RunSeed(int seed)
    {
        var rng = new Random(seed);
        var n = rng.Next(0, 400);
        var failFast = rng.Next(2) == 0;
        var rates = new[] { 0.0, 0.0, 0.05, 0.3, 0.9 };
        var stageCount = rng.Next(0, 5);
        var specs = Enumerable.Range(0, stageCount).Select(_ =>
        {
            var par = rng.Next(1, 7);
            return new Spec(rng.Next(1, 9), par, rng.Next(2) == 0, rng.Next(1, 4), rates[rng.Next(rates.Length)],
                rng.Next(3) == 0 ? par + rng.Next(0, 4) : null);
        }).ToList();
        var sinkSpec = new Spec(rng.Next(1, 7), rng.Next(1, 4), true, rng.Next(1, 4), rng.Next(3) == 0 ? 0.1 : 0.0, null);
        var allSpecs = specs.Append(sinkSpec).ToList();
        int? cancelAt = rng.Next(3) == 0 ? rng.Next(1, Math.Max(2, n * (stageCount + 1) / 2)) : null;

        bool Fails(int stage, int item, int attempt) =>
            Kit.Hash01(seed, stage, item, attempt) < allSpecs[stage].FailRate;
        // Oracle: the stage at which an item dies (all attempts fail), or -1 if it survives everything.
        int DeathStage(int item)
        {
            for (var s = 0; s < allSpecs.Count; s++)
            {
                var all = true;
                for (var a = 1; a <= allSpecs[s].Attempts; a++) all &= Fails(s, item, a);
                if (all) return s;
            }

            return -1;
        }

        string Name(int stage) => stage == stageCount ? "sink" : "s" + stage;

        using var cts = new CancellationTokenSource();
        var invocations = new Counter();
        var attemptCounts = new ConcurrentDictionary<(int, int), int>();
        var disposed = new Flag();
        var collected = new Collector<int>();

        async ValueTask<int> Work(int stage, int item, CancellationToken ct)
        {
            if (cancelAt is { } c && invocations.Increment() == c) cts.Cancel();
            var attempt = attemptCounts.AddOrUpdate((stage, item), 1, (_, v) => v + 1);
            var yields = (int)(Kit.Hash01(seed, stage, item, 99 + attempt) * 4);
            for (var y = 0; y < yields; y++) await Task.Yield();
            if (Fails(stage, item, attempt)) throw new InvalidOperationException($"s{stage} item {item} attempt {attempt}");
            return item;
        }

        var options = new PipelineOptions
        {
            FailureMode = failFast ? FailureMode.FailFast : FailureMode.Continue,
            SinkOptions = new StageOptions { Capacity = sinkSpec.Capacity, Parallelism = sinkSpec.Parallelism, Retry = new RetryPolicy(sinkSpec.Attempts) },
        };
        var builder = Pipeline.From(Kit.Range(n, disposed: disposed), options);
        PipelineBuilder<int> chain = builder;
        for (var s = 0; s < stageCount; s++)
        {
            var stage = s;
            var spec = specs[s];
            chain = chain.Stage(Name(s), (int x, CancellationToken ct) => Work(stage, x, ct), new StageOptions
            {
                Capacity = spec.Capacity, Parallelism = spec.Parallelism, Ordered = spec.Ordered,
                Retry = new RetryPolicy(spec.Attempts), ReorderWindow = spec.Window,
            });
        }

        var report = await chain.RunAsync(async (x, ct) =>
        {
            await Work(stageCount, x, ct);
            collected.Add(x);
        }, cts.Token);

        // --- invariants that hold for every shape and cancel point
        Assertions.AssertBalanced(report);
        Assert.True(disposed.IsSet, "source not disposed");
        Assertions.AssertAllInputsCompleted(report);
        Assert.All(report.Stages, s => Assert.Equal(0, s.BusyWorkers));
        var got = collected.Items;
        Assert.Equal(report.Delivered, got.Count);
        Assert.Equal(got.Count, got.Distinct().Count());
        Assert.All(got, x => Assert.Equal(-1, DeathStage(x)));
        Assert.All(report.DeadLetters, d =>
        {
            var item = (int)d.Item!;
            Assert.Equal(DeathStage(item), allSpecs.Count > 0 ? Array.IndexOf(Enumerable.Range(0, allSpecs.Count).Select(Name).ToArray(), d.Stage) : -2);
            Assert.Equal(allSpecs[DeathStage(item)].Attempts, d.Attempts);
            Assert.Equal(d.Sequence, item);
        });
        Assert.Equal(report.DeadLettered, report.DeadLetters.Count); // far below the 1000 cap
        var deliveredSeqs = got.Select(x => (long)x).ToHashSet();
        Assert.DoesNotContain(report.NotProcessedSequences, deliveredSeqs.Contains);
        foreach (var d in report.DeadLetters) Assert.DoesNotContain(d.Sequence, report.NotProcessedSequences);

        var allInOrder = specs.All(s => s.Ordered || s.Parallelism == 1) && sinkSpec.Parallelism == 1;
        if (allInOrder) Assert.Equal(got.OrderBy(x => x), got);

        foreach (var s in report.Stages)
        {
            Assert.InRange(s.MaxQueueDepth, 0, s.QueueCapacity);
            if (s.ReorderWindow > 0) Assert.True(s.MaxReorderBuffered <= s.ReorderWindow);
        }

        // --- outcome-specific
        switch (report.Outcome)
        {
            case RunOutcome.Completed:
                Assert.Equal(n, report.Accepted);
                Assert.Equal(0, report.NotProcessed);
                var expectedDead = Enumerable.Range(0, n).Count(i => DeathStage(i) >= 0);
                Assert.Equal(expectedDead, report.DeadLettered);
                Assert.Equal(n - expectedDead, report.Delivered);
                break;
            case RunOutcome.Cancelled:
                Assert.Null(report.FatalError);
                Assert.NotNull(cancelAt);
                break;
            case RunOutcome.Faulted:
                Assert.True(failFast);
                var fatal = Assert.IsType<StageFailedException>(report.FatalError);
                Assert.True(report.DeadLettered >= 1);
                Assert.Contains(report.DeadLetters, d => d.Sequence == fatal.Sequence && d.Stage == fatal.Stage);
                break;
        }

        // A run without cancellation and without any oracle failure must be a complete success.
        if (cancelAt is null && Enumerable.Range(0, n).All(i => DeathStage(i) < 0))
        {
            Assert.Equal(RunOutcome.Completed, report.Outcome);
            Assert.Equal(n, report.Delivered);
        }
    }
}
