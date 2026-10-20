namespace ChannelsPipeline.Tests;

public class BasicTests
{
    [Fact(Timeout = Limits.Ms)]
    public async Task EmptySource_CompletesWithEmptyBalancedReport()
    {
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(0)).Stage("a", x => x + 1).RunAsync(sink.Sink);

        Assert.Equal(RunOutcome.Completed, report.Outcome);
        Assert.Null(report.FatalError);
        Assert.Equal(0, report.Accepted);
        Assert.Equal(0, report.Delivered);
        Assert.Empty(sink.Items);
        Assert.Empty(report.NotProcessedSequences);
        Assertions.AssertBalanced(report);
        Assertions.AssertAllInputsCompleted(report);
        Assert.All(report.Stages, s => Assert.Equal(0, s.In));
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task EmptySource_WithZeroStages_Completes()
    {
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(0)).RunAsync(sink.Sink);
        Assert.Equal(RunOutcome.Completed, report.Outcome);
        Assert.Single(report.Stages);
        Assert.Equal("sink", report.Stages[0].Name);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SourceOnly_DeliversEverythingToSink()
    {
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(25)).RunAsync(sink.Sink);
        Assert.Equal(Enumerable.Range(0, 25), sink.Items);
        Assert.Equal(25, report.Delivered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SingleStage_Transforms()
    {
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(10)).Stage("double", x => x * 2).RunAsync(sink.Sink);
        Assert.Equal(Enumerable.Range(0, 10).Select(x => x * 2), sink.Items);
        Assert.Equal(10, report.Delivered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task Stages_ChangeTypesAlongTheChain()
    {
        var sink = new Collector<double>();
        var report = await Pipeline.From(Kit.Range(5))
            .Stage("toString", x => $"#{x}")
            .Stage("length", s => s.Length)
            .Stage("half", async (int n, CancellationToken _) => { await Task.Yield(); return n / 2.0; })
            .RunAsync(sink.Sink);
        Assert.Equal(new[] { 1.0, 1.0, 1.0, 1.0, 1.0 }, sink.Items);
        Assert.Equal(new[] { "toString", "length", "half", "sink" }, report.Stages.Select(s => s.Name));
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SynchronousSource_And_SynchronousSink_Overloads()
    {
        var seen = new List<string>();
        var report = await Pipeline.From(new[] { "a", "b", "c" }).Stage("up", s => s.ToUpperInvariant()).RunAsync(s => seen.Add(s));
        Assert.Equal(new[] { "A", "B", "C" }, seen);
        Assert.Equal(3, report.Delivered);
    }

    [Theory(Timeout = Limits.Ms)]
    [InlineData(0, 1, true)] [InlineData(0, 4, true)] [InlineData(1, 1, true)] [InlineData(1, 4, false)]
    [InlineData(2, 1, false)] [InlineData(2, 4, true)] [InlineData(17, 1, true)] [InlineData(17, 4, true)]
    [InlineData(100, 1, false)] [InlineData(100, 4, false)] [InlineData(100, 4, true)] [InlineData(1000, 1, true)]
    [InlineData(1000, 4, true)] [InlineData(1000, 4, false)] [InlineData(1000, 16, true)] [InlineData(1000, 16, false)]
    public async Task EveryItemIsDeliveredExactlyOnce(int count, int parallelism, bool ordered)
    {
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(count))
            .Stage("s", async (int x, CancellationToken _) => { if (x % 3 == 0) await Task.Yield(); return x; },
                new StageOptions { Parallelism = parallelism, Ordered = ordered, Capacity = 3 })
            .RunAsync(sink.Sink);

        var got = sink.Items;
        if (ordered) Assert.Equal(Enumerable.Range(0, count), got);
        else Assert.Equal(Enumerable.Range(0, count), got.OrderBy(x => x));
        Assert.Equal(count, report.Accepted);
        Assert.Equal(count, report.Delivered);
        Assertions.AssertBalanced(report);
        Assertions.AssertAllInputsCompleted(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task BuildingAPipeline_DoesNotPullFromTheSource()
    {
        var pulled = new Counter();
        var builder = Pipeline.From(Kit.Range(10, pulled)).Stage("a", x => x).Stage("b", x => x);
        await Task.Yield();
        Assert.Equal(0, pulled.Value);
        var report = await builder.RunAsync(_ => { });
        Assert.Equal(10, pulled.Value);
        Assert.Equal(10, report.Delivered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task Builder_CanRunTwice()
    {
        var builder = Pipeline.From(new[] { 1, 2, 3 }).Stage("inc", x => x + 1);
        var a = new Collector<int>();
        var b = new Collector<int>();
        await builder.RunAsync(a.Sink);
        await builder.RunAsync(b.Sink);
        Assert.Equal(new[] { 2, 3, 4 }, a.Items);
        Assert.Equal(a.Items, b.Items);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task Start_ExposesLiveSnapshotAndFinalReportMatches()
    {
        var gate = new Gate();
        var entered = new Signal();
        var sink = new Collector<int>();
        var run = Pipeline.From(Kit.Range(3))
            .Stage("hold", async (int x, CancellationToken ct) => { entered.Increment(); await gate.WaitAsync(ct); return x; })
            .Start(sink.Sink);

        await entered.AtLeastAsync(1);
        var live = run.Snapshot();
        Assert.Equal(2, live.Stages.Count);
        Assert.Equal("hold", live.Stages[0].Name);
        Assert.Equal(1, live.Stages[0].BusyWorkers);

        gate.Open();
        var report = await run.Completion;
        Assert.Equal(3, report.Delivered);
        Assert.Equal(3, run.Snapshot().Delivered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task CompletedReport_ThrowIfNotCompleted_DoesNotThrow()
    {
        var report = await Pipeline.From(Kit.Range(2)).RunAsync(_ => { });
        report.ThrowIfNotCompleted();
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task Stage_RunsItsFunctionOffTheCallersContext_AndKeepsWorkersBusyConcurrently()
    {
        // Barrier: 4 workers must be inside the function at the same time, proving real parallelism.
        var barrier = new Barrier(4);
        var report = await Pipeline.From(Kit.Range(4))
            .Stage("par", async (int x, CancellationToken _) =>
            {
                await Task.Run(() => barrier.SignalAndWait(TimeSpan.FromSeconds(30)) ? 0 : throw new TimeoutException());
                return x;
            }, new StageOptions { Parallelism = 4, Capacity = 4 })
            .RunAsync(_ => { });
        Assert.Equal(4, report.Delivered);
        Assert.Equal(4, report.Stages[0].MaxBusyWorkers);
    }
}
