using System.Threading.Channels;

namespace ChannelsPipeline.Tests;

/// <summary>Tests added after the first mutation run showed what the suite did not pin down (see the README's mutation section).</summary>
public class ArgumentValidationTests
{
    private static PipelineBuilder<int> P() => Pipeline.From(new[] { 1 });

    [Fact(Timeout = Limits.Ms)]
    public void From_Enumerable_RejectsNull() =>
        Assert.Throws<ArgumentNullException>(() => Pipeline.From((IEnumerable<int>)null!));

    [Fact(Timeout = Limits.Ms)]
    public void From_RejectsNullTimeProvider() =>
        Assert.Throws<ArgumentNullException>(() => Pipeline.From(new[] { 1 }, new PipelineOptions { TimeProvider = null! }));

    [Fact(Timeout = Limits.Ms)]
    public void From_RejectsNullSinkOptions() =>
        Assert.Throws<ArgumentNullException>(() => Pipeline.From(new[] { 1 }, new PipelineOptions { SinkOptions = null! }));

    [Fact(Timeout = Limits.Ms)]
    public void Stage_AsyncOverload_RejectsNullFunction() =>
        Assert.Throws<ArgumentNullException>(() => P().Stage<int>("a", (Func<int, CancellationToken, ValueTask<int>>)null!));

    [Fact(Timeout = Limits.Ms)]
    public void Stage_RejectsNullRetryPolicy() =>
        Assert.Throws<ArgumentNullException>(() => P().Stage("a", x => x, new StageOptions { Retry = null! }));

    [Fact(Timeout = Limits.Ms)]
    public void Start_RejectsNullSink() =>
        Assert.Throws<ArgumentNullException>(() => P().Start(null!));

    [Fact(Timeout = Limits.Ms)]
    public void RunAsync_SynchronousOverload_RejectsNullSink() =>
        Assert.Throws<ArgumentNullException>(() => { _ = P().RunAsync((Action<int>)null!); });

    [Fact(Timeout = Limits.Ms)]
    public void Merge_RejectsNullSourceList() =>
        Assert.Throws<ArgumentNullException>(() => Fan.Merge((IReadOnlyList<IAsyncEnumerable<int>>)null!, 4));

    [Fact(Timeout = Limits.Ms)]
    public void Merge_RejectsNullSourceInTheList() =>
        Assert.Throws<ArgumentNullException>(() => Fan.Merge(new IAsyncEnumerable<int>[] { Kit.Range(1), null! }, 4));
}

public class MessageTests
{
    private static PipelineBuilder<int> P() => Pipeline.From(new[] { 1 });

    [Fact(Timeout = Limits.Ms)]
    public void ValidationMessages_NameTheStageAndTheProblem()
    {
        var cap = Assert.Throws<ArgumentOutOfRangeException>(() => P().Stage("cap-stage", x => x, new StageOptions { Capacity = 0 }));
        Assert.Contains("cap-stage", cap.Message);
        Assert.Contains("capacity", cap.Message);
        var par = Assert.Throws<ArgumentOutOfRangeException>(() => P().Stage("par-stage", x => x, new StageOptions { Parallelism = 0 }));
        Assert.Contains("par-stage", par.Message);
        Assert.Contains("parallelism", par.Message);
        var win = Assert.Throws<ArgumentOutOfRangeException>(() => P().Stage("win-stage", x => x, new StageOptions { Parallelism = 3, ReorderWindow = 2 }));
        Assert.Contains("win-stage", win.Message);
        Assert.Contains("window", win.Message);
        var to = Assert.Throws<ArgumentOutOfRangeException>(() => P().Stage("to-stage", x => x, new StageOptions { ItemTimeout = TimeSpan.Zero }));
        Assert.Contains("to-stage", to.Message);
        Assert.Contains("timeout", to.Message);
    }

    [Fact(Timeout = Limits.Ms)]
    public void SinkValidationMessage_SaysItIsTheSink()
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => Pipeline.From(new[] { 1 }, new PipelineOptions { SinkOptions = new StageOptions { Capacity = 0 } }));
        Assert.Contains("sink", ex.Message);
    }

    [Fact(Timeout = Limits.Ms)]
    public void DuplicateNameMessage_NamesTheStage()
    {
        var ex = Assert.Throws<ArgumentException>(() => P().Stage("dup", x => x).Stage("dup", x => x));
        Assert.Contains("dup", ex.Message);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task FatalErrorMessages_CarryTheirContext()
    {
        // source failure
        var src = await Pipeline.From(Kit.ThrowingSource(0, new InvalidOperationException("the-root-cause"))).RunAsync(_ => { });
        Assert.Contains("source", src.FatalError!.Message);
        Assert.Contains("the-root-cause", src.FatalError.Message);
        var failed = Assert.Throws<PipelineFailedException>(src.ThrowIfNotCompleted);
        Assert.Contains("faulted", failed.Message);
        Assert.Contains("the-root-cause", failed.Message);

        // dead-letter sink failure
        var dl = await Pipeline.From(new[] { 1 }, new PipelineOptions { DeadLetterSink = new DelegateDeadLetterSink((_, _) => throw new IOException("disk-gone")) })
            .Stage<int>("f", (x, _) => throw new Exception("e")).RunAsync(_ => { });
        Assert.Contains("dead-letter", dl.FatalError!.Message);
        Assert.Contains("disk-gone", dl.FatalError.Message);

        // internal error
        var internalError = await Pipeline.From(new[] { 1 })
            .Stage<int>("f", (x, _) => throw new Exception("e"), new StageOptions { Retry = new RetryPolicy(2, shouldRetry: _ => throw new InvalidOperationException("predicate-bug")) })
            .RunAsync(_ => { });
        Assert.Contains("'f'", internalError.FatalError!.Message);
        Assert.Contains("predicate-bug", internalError.FatalError.Message);
    }

    [Fact(Timeout = Limits.Ms)]
    public void TimeoutAndCancellationMessages_AreInformative()
    {
        var timeout = new ItemTimeoutException("slow-stage", TimeSpan.FromSeconds(2));
        Assert.Contains("slow-stage", timeout.Message);
        Assert.Contains("timeout", timeout.Message);
        var report = new RunReport
        {
            Outcome = RunOutcome.Cancelled, Accepted = 0, Delivered = 0, DeadLettered = 0,
            NotProcessedSequences = Array.Empty<long>(), DeadLetters = Array.Empty<DeadLetter>(),
            Stages = Array.Empty<StageMetrics>(), Elapsed = TimeSpan.Zero,
        };
        var ex = Assert.Throws<OperationCanceledException>(report.ThrowIfNotCompleted);
        Assert.Contains("cancelled", ex.Message);
    }
}

public class GapTests
{
    [Fact(Timeout = Limits.Ms)]
    public void ExponentialBackoff_ClampsHugeDelaysInsteadOfOverflowing()
    {
        var policy = RetryPolicy.Exponential(100, TimeSpan.FromDays(30_000));
        var delay = policy.Backoff(31);
        Assert.True(delay > TimeSpan.Zero, $"delay overflowed: {delay}");
        Assert.True(delay <= TimeSpan.FromTicks(1L << 62));
    }

    [Fact(Timeout = Limits.Ms)]
    public void ReorderWindowDefault_DetectsIntegerOverflow() =>
        Assert.Throws<OverflowException>(() => new StageOptions { Parallelism = int.MaxValue }.EffectiveReorderWindow);

    [Fact(Timeout = Limits.Ms)]
    public async Task NotProcessedSequences_AreAscending()
    {
        // 1000 items flow through, then 50+ park: the pending keys (1000..) wrap around the internal hash table, so only an
        // explicit sort makes the reported list ascending.
        var entered = new Signal();
        using var cts = new CancellationTokenSource();
        var run = Pipeline.From(Kit.Range(1_000_000))
            .Stage("park", async (int x, CancellationToken ct) =>
            {
                if (x >= 1000)
                {
                    entered.Increment();
                    await Task.Delay(Timeout.Infinite, ct);
                }

                return x;
            }, new StageOptions { Capacity = 50, Parallelism = 2, Ordered = false })
            .Start((_, _) => ValueTask.CompletedTask, cts.Token);
        await entered.AtLeastAsync(2);
        await Kit.UntilAsync(() => run.Snapshot().Stages[0].QueueDepth == 50);
        cts.Cancel();
        var report = await run.Completion;
        Assert.True(report.NotProcessedSequences.Count >= 50);
        Assert.Equal(report.NotProcessedSequences.OrderBy(x => x), report.NotProcessedSequences);
        Assert.Equal(Enumerable.Range(1000, report.NotProcessedSequences.Count).Select(i => (long)i), report.NotProcessedSequences);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task UserOperationCanceled_WithATimeoutConfigured_IsNotReportedAsATimeout()
    {
        var clock = new TestClock();
        var report = await Pipeline.From(new[] { 1 }, new PipelineOptions { TimeProvider = clock })
            .Stage<int>("f", (x, _) => throw new OperationCanceledException("cancelled by the function itself"),
                new StageOptions { ItemTimeout = TimeSpan.FromHours(1) })
            .RunAsync(_ => { });
        var dl = Assert.Single(report.DeadLetters);
        Assert.IsType<OperationCanceledException>(dl.Exception);
        Assert.IsNotType<ItemTimeoutException>(dl.Exception);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task StageTotalTime_StopsAtTheEndOfTheStage_WhileLiveValuesKeepGrowing()
    {
        var clock = new TestClock();
        var entered = new Signal();
        var gate = new Gate();
        var run = Pipeline.From(new[] { 1 }, new PipelineOptions { TimeProvider = clock })
            .Stage("t", async (int x, CancellationToken ct) => { entered.Increment(); await gate.WaitAsync(ct); return x; })
            .Start((_, _) => ValueTask.CompletedTask);
        await entered.AtLeastAsync(1);
        clock.Advance(TimeSpan.FromSeconds(3));
        Assert.Equal(TimeSpan.FromSeconds(3), run.Snapshot().Stages[0].TotalTime);
        clock.Advance(TimeSpan.FromSeconds(2));
        Assert.Equal(TimeSpan.FromSeconds(5), run.Snapshot().Stages[0].TotalTime);   // live: grows while running
        gate.Open();
        var report = await run.Completion;
        clock.Advance(TimeSpan.FromHours(1));
        Assert.Equal(TimeSpan.FromSeconds(5), report.Stages[0].TotalTime);
        Assert.Equal(TimeSpan.FromSeconds(5), run.Snapshot().Stages[0].TotalTime);    // final: frozen at the stage's end
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ReorderBufferMetric_DoesNotCountTheHeadThatIsAboutToBeEmitted()
    {
        // Items 0 and 1, two workers, window 2. Item 1 finishes first and waits (1 buffered); then item 0 finishes: at that
        // moment the dictionary holds both, but item 0 is emitted by the finishing worker, so only item 1 is "waiting".
        var gate = new Gate();
        var sink = new Collector<int>();
        var run = Pipeline.From(Kit.Range(2))
            .Stage("pair", async (int x, CancellationToken ct) => { if (x == 0) await gate.WaitAsync(ct); return x; },
                new StageOptions { Parallelism = 2, ReorderWindow = 2, Capacity = 2 })
            .Start(sink.Sink);
        await Kit.UntilAsync(() => run.Snapshot().Stages[0].ReorderBuffered == 1);
        gate.Open();
        var report = await run.Completion;
        Assert.Equal(new[] { 0, 1 }, sink.Items);
        Assert.Equal(1, report.Stages[0].MaxReorderBuffered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task Batch_SizeFlush_CancelsAndDisposesItsTimer()
    {
        var clock = new TestClock();
        var channel = Channel.CreateUnbounded<int>();
        var resumed = new Signal();
        async IAsyncEnumerable<int> Source([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            await foreach (var x in channel.Reader.ReadAllAsync(ct)) { yield return x; resumed.Increment(); }
        }

        await using var batches = Source().Batch(3, TimeSpan.FromSeconds(5), clock).GetAsyncEnumerator();
        var next = batches.MoveNextAsync().AsTask();
        for (var i = 0; i < 3; i++) channel.Writer.TryWrite(i);
        Assert.True(await next);
        Assert.Equal(new[] { 0, 1, 2 }, batches.Current);
        Assert.Equal(1, clock.TimersCreated.Count);
        Assert.Equal(1, clock.TimersDisposed.Count);   // the pending time-flush was cancelled, not left running
    }
}

public class MutationFollowUpTests
{
    [Fact(Timeout = Limits.Ms)]
    public async Task ExponentialBackoff_ArmsItsTimersWithDoublingDueTimes()
    {
        var clock = new TestClock();
        var run = Pipeline.From(new[] { 1 }, new PipelineOptions { TimeProvider = clock })
            .Stage<int>("f", (x, _) => throw new Exception("x"), new StageOptions { Retry = RetryPolicy.Exponential(4, TimeSpan.FromSeconds(1)) })
            .Start((_, _) => ValueTask.CompletedTask);
        foreach (var k in new[] { 1, 2, 3 })
        {
            await clock.TimersCreated.AtLeastAsync(k);
            clock.Advance(TimeSpan.FromSeconds(Math.Pow(2, k - 1)));
        }

        await run.Completion;
        Assert.Equal(new[] { 1, 2, 4 }.Select(s => TimeSpan.FromSeconds(s)), clock.DueTimes);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task FirstFatalError_IsTheOneReported_WhenASecondOneFollows()
    {
        var gate = new Gate();
        var options = new PipelineOptions { DeadLetterSink = new DelegateDeadLetterSink((_, _) => throw new IOException("dl sink down")) };
        var run = Pipeline.From(Kit.ThrowingSource(1, new InvalidOperationException("source broke")), options)
            .Stage<int>("f", async (x, ct) =>
            {
                await gate.WaitAsync(ct);
                throw new Exception("item fails");
            })
            .Start((_, _) => ValueTask.CompletedTask);
        await Kit.UntilAsync(() => run.Snapshot().Stages[0].InputCompleted);   // the pump has recorded the source failure
        gate.Open();                                                          // now the item fails and the dead-letter sink throws
        var report = await run.Completion;
        Assert.Equal(RunOutcome.Faulted, report.Outcome);
        Assert.IsType<SourceFailedException>(report.FatalError);
    }
}
