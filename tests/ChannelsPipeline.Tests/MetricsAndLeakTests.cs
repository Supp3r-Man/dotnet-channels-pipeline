namespace ChannelsPipeline.Tests;

public class MetricsTests
{
    [Theory(Timeout = Limits.Ms)]
    [InlineData(1, 1)] [InlineData(2, 4)] [InlineData(3, 6)]
    public async Task OnCompletion_EveryStageSatisfies_In_Equals_Out_Plus_Failed(int seed, int parallelism)
    {
        var report = await Pipeline.From(Kit.Range(600), new PipelineOptions { SinkOptions = new StageOptions { Parallelism = parallelism } })
            .Stage("a", x => x % 11 == 0 ? throw new Exception("a") : x, new StageOptions { Parallelism = parallelism, Retry = new RetryPolicy(2) })
            .Stage("b", x => x % 13 == seed ? throw new Exception("b") : x, new StageOptions { Parallelism = parallelism, Ordered = false })
            .RunAsync((x, _) => x % 17 == 0 ? throw new Exception("sink") : ValueTask.CompletedTask);

        Assert.All(report.Stages, s => Assert.Equal(s.In, s.Out + s.Failed));
        Assert.Equal(report.Accepted, report.Stages[0].In);
        Assert.Equal(report.Stages[0].Out, report.Stages[1].In);
        Assert.Equal(report.Stages[1].Out, report.Stages[2].In);
        Assert.Equal(report.Delivered, report.Stages[2].Out);
        Assert.Equal(report.DeadLettered, report.Stages.Sum(s => s.Failed));
        Assert.All(report.Stages, s => Assert.Equal(0, s.QueueDepth));
        Assertions.AssertBalanced(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task BusyWorkers_ReachesParallelism_WhenAllWorkersAreParked()
    {
        var entered = new Signal();
        var gate = new Gate();
        var run = Pipeline.From(Kit.Range(50))
            .Stage("p", async (int x, CancellationToken ct) => { entered.Increment(); await gate.WaitAsync(ct); return x; },
                new StageOptions { Parallelism = 5, Capacity = 2, Ordered = false })
            .Start((_, _) => ValueTask.CompletedTask);
        await entered.AtLeastAsync(5);
        Assert.Equal(5, run.Snapshot().Stages[0].BusyWorkers);
        gate.Open();
        var report = await run.Completion;
        Assert.Equal(5, report.Stages[0].MaxBusyWorkers);
        Assert.Equal(0, report.Stages[0].BusyWorkers);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task Timings_UseTheInjectedClock()
    {
        var clock = new TestClock();
        var entered = new Signal();
        var gate = new Gate();
        var run = Pipeline.From(new[] { 1 }, new PipelineOptions { TimeProvider = clock })
            .Stage("t", async (int x, CancellationToken ct) => { entered.Increment(); await gate.WaitAsync(ct); return x; })
            .Start((_, _) => ValueTask.CompletedTask);
        await entered.AtLeastAsync(1);
        clock.Advance(TimeSpan.FromSeconds(3));
        Assert.Equal(TimeSpan.FromSeconds(3), run.Snapshot().Stages[0].TotalTime);   // live: still running
        gate.Open();
        var report = await run.Completion;
        Assert.Equal(TimeSpan.FromSeconds(3), report.Stages[0].BusyTime);
        Assert.Equal(TimeSpan.FromSeconds(3), report.Stages[0].TotalTime);
        Assert.Equal(TimeSpan.FromSeconds(3), report.Elapsed);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task LiveSnapshot_NeverShowsDeliveredAboveAccepted()
    {
        var run = Pipeline.From(Kit.Range(5000))
            .Stage("a", async (int x, CancellationToken _) => { await Task.Yield(); return x; }, new StageOptions { Parallelism = 4 })
            .Start((_, _) => ValueTask.CompletedTask);
        while (!run.Completion.IsCompleted)
        {
            var s = run.Snapshot();
            Assert.True(s.Delivered <= s.Accepted, $"delivered {s.Delivered} > accepted {s.Accepted}");
            Assert.All(s.Stages, m => Assert.InRange(m.QueueDepth, 0, m.QueueCapacity));
            await Task.Yield();
        }

        Assert.Equal(5000, (await run.Completion).Delivered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task Metrics_ReportConfiguration()
    {
        var report = await Pipeline.From(Kit.Range(3))
            .Stage("cfg", x => x, new StageOptions { Capacity = 9, Parallelism = 3, ReorderWindow = 7 })
            .RunAsync(_ => { });
        var m = report.Stages[0];
        Assert.Equal((9, 3, 7), (m.QueueCapacity, m.Parallelism, m.ReorderWindow));
    }
}

public class LeakTests
{
    [Fact(Timeout = Limits.Ms)]
    public async Task NormalRun_DisposesSource_CompletesEveryQueue_AndFinishesItsTask()
    {
        var disposed = new Flag();
        var run = Pipeline.From(Kit.Range(300, disposed: disposed))
            .Stage("a", x => x, new StageOptions { Parallelism = 3 })
            .Stage("b", x => x, new StageOptions { Parallelism = 2, Ordered = false })
            .Start((_, _) => ValueTask.CompletedTask);
        var report = await run.Completion;
        Assert.True(run.Completion.IsCompletedSuccessfully);
        Assert.True(disposed.IsSet);
        Assertions.AssertAllInputsCompleted(report);
        Assert.All(report.Stages, s => Assert.Equal(0, s.BusyWorkers));
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task FailFastRun_CleansUpToo()
    {
        var disposed = new Flag();
        var report = await Pipeline.From(Kit.Range(1_000_000, disposed: disposed), new PipelineOptions { FailureMode = FailureMode.FailFast })
            .Stage<int>("a", (x, _) => x == 10 ? throw new Exception("x") : new ValueTask<int>(x), new StageOptions { Parallelism = 3, Capacity = 2 })
            .Stage("b", x => x)
            .RunAsync(_ => { });
        Assert.Equal(RunOutcome.Faulted, report.Outcome);
        Assert.True(disposed.IsSet);
        Assertions.AssertAllInputsCompleted(report);
        Assert.All(report.Stages, s => Assert.Equal(0, s.BusyWorkers));
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SourceError_CleansUpToo()
    {
        var disposed = new Flag();
        var report = await Pipeline.From(Kit.ThrowingSource(7, new Exception("src"), disposed))
            .Stage("a", x => x, new StageOptions { Parallelism = 2 })
            .RunAsync(_ => { });
        Assert.True(disposed.IsSet);
        Assertions.AssertAllInputsCompleted(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ManyShortRuns_AllFinish_NoneLeavesWorkBehind()
    {
        for (var i = 0; i < 200; i++)
        {
            var mode = i % 3 == 0 ? FailureMode.FailFast : FailureMode.Continue;
            var disposed = new Flag();
            var report = await Pipeline.From(Kit.Range(i % 40, disposed: disposed), new PipelineOptions { FailureMode = mode })
                .Stage("a", async (int x, CancellationToken _) => { await Task.Yield(); return x % 7 == 5 ? throw new Exception("e") : x; },
                    new StageOptions { Parallelism = 1 + i % 5, Capacity = 1 + i % 3 })
                .RunAsync(_ => { });
            Assert.True(disposed.IsSet);
            Assertions.AssertBalanced(report);
            Assertions.AssertAllInputsCompleted(report);
        }
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ForgottenRunHandle_StillFinishes_WhenTheSourceEnds()
    {
        var sink = new Collector<int>();
        var run = Pipeline.From(Kit.Range(10)).Start(sink.Sink);
        await run.Completion;
        Assert.Equal(10, sink.Count);
    }
}
