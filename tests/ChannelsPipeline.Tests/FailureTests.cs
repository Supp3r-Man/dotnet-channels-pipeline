namespace ChannelsPipeline.Tests;

public class FailureModeTests
{
    [Theory(Timeout = Limits.Ms)]
    [InlineData(1)] [InlineData(4)]
    public async Task Continue_DeadLettersFailures_AndDeliversTheRest(int parallelism)
    {
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(500), new PipelineOptions { FailureMode = FailureMode.Continue })
            .Stage("f", async (int x, CancellationToken _) => { await Task.Yield(); if (x % 10 == 3) throw new InvalidOperationException(); return x; },
                new StageOptions { Parallelism = parallelism })
            .RunAsync(sink.Sink);

        Assert.Equal(RunOutcome.Completed, report.Outcome);
        Assert.Null(report.FatalError);
        Assert.Equal(50, report.DeadLettered);
        Assert.Equal(450, report.Delivered);
        Assert.Equal(Enumerable.Range(0, 500).Where(x => x % 10 != 3), sink.Items.OrderBy(x => x));
        Assertions.AssertBalanced(report);
        Assert.Empty(report.NotProcessedSequences);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task Continue_WhenEveryItemFails_StillCompletes()
    {
        var report = await Pipeline.From(Kit.Range(300))
            .Stage<int>("f", (x, _) => throw new Exception("all bad"), new StageOptions { Parallelism = 3, Retry = new RetryPolicy(2) })
            .RunAsync(_ => { });
        Assert.Equal(RunOutcome.Completed, report.Outcome);
        Assert.Equal(300, report.DeadLettered);
        Assert.Equal(0, report.Delivered);
        Assert.Equal(300, report.Stages[0].Failed);
        Assert.Equal(300, report.Stages[0].Retried);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task FailFast_StopsAtTheFirstExhaustedItem_AndReportsEverythingElseAsNotProcessed()
    {
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(1_000_000), new PipelineOptions { FailureMode = FailureMode.FailFast })
            .Stage("f", (int x) => x == 3 ? throw new InvalidOperationException("bad three") : x,
                new StageOptions { Retry = new RetryPolicy(2), Capacity = 4 })
            .RunAsync(sink.Sink);

        Assert.Equal(RunOutcome.Faulted, report.Outcome);
        var fatal = Assert.IsType<StageFailedException>(report.FatalError);
        Assert.Equal("f", fatal.Stage);
        Assert.Equal(3, fatal.Sequence);
        Assert.Equal(2, fatal.Attempts);
        Assert.IsType<InvalidOperationException>(fatal.InnerException);
        Assert.Equal(1, report.DeadLettered);
        Assert.Equal(3, report.DeadLetters[0].Sequence);
        Assert.True(report.Accepted < 100, "source must stop being pulled");
        Assert.True(report.Delivered <= 3);
        Assertions.AssertBalanced(report);
        Assert.DoesNotContain(3L, report.NotProcessedSequences);
        Assertions.AssertAllInputsCompleted(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task FailFast_AbandonsItemsInFlightInOtherWorkers()
    {
        var blocked = new Signal();
        var report = await Pipeline.From(Kit.Range(1000), new PipelineOptions { FailureMode = FailureMode.FailFast })
            .Stage("f", async (int x, CancellationToken ct) =>
            {
                if (x == 3)
                {
                    await blocked.AtLeastAsync(3);   // items 0,1,2 are parked in the other workers
                    throw new InvalidOperationException("boom");
                }

                blocked.Increment();
                await Task.Delay(Timeout.Infinite, ct);
                return x;
            }, new StageOptions { Parallelism = 4, Capacity = 4 })
            .RunAsync(_ => { });

        Assert.Equal(RunOutcome.Faulted, report.Outcome);
        Assert.Equal(1, report.DeadLettered);
        Assert.Contains(0L, report.NotProcessedSequences);
        Assert.Contains(1L, report.NotProcessedSequences);
        Assert.Contains(2L, report.NotProcessedSequences);
        Assertions.AssertBalanced(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task FailFast_WithNoFailures_Completes()
    {
        var report = await Pipeline.From(Kit.Range(100), new PipelineOptions { FailureMode = FailureMode.FailFast })
            .Stage("f", x => x).RunAsync(_ => { });
        Assert.Equal(RunOutcome.Completed, report.Outcome);
        Assert.Equal(100, report.Delivered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ThrowIfNotCompleted_ForFaultedRun_ThrowsPipelineFailedException_WithTheFatalErrorInside()
    {
        var report = await Pipeline.From(Kit.Range(10), new PipelineOptions { FailureMode = FailureMode.FailFast })
            .Stage<int>("f", (x, _) => throw new FormatException("fmt")).RunAsync(_ => { });
        var ex = Assert.Throws<PipelineFailedException>(report.ThrowIfNotCompleted);
        Assert.Same(report, ex.Report);
        Assert.IsType<StageFailedException>(ex.InnerException);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SourceThatThrows_Continue_DrainsAcceptedItems_ThenReportsFaulted()
    {
        var disposed = new Flag();
        var sink = new Collector<int>();
        var boom = new InvalidOperationException("source broke");
        var report = await Pipeline.From(Kit.ThrowingSource(10, boom, disposed))
            .Stage("f", x => x + 1, new StageOptions { Parallelism = 3 })
            .RunAsync(sink.Sink);

        Assert.Equal(RunOutcome.Faulted, report.Outcome);
        var fatal = Assert.IsType<SourceFailedException>(report.FatalError);
        Assert.Same(boom, fatal.InnerException);
        Assert.Equal(10, report.Accepted);
        Assert.Equal(10, report.Delivered);
        Assert.Equal(Enumerable.Range(1, 10), sink.Items);
        Assert.True(disposed.IsSet);
        Assertions.AssertBalanced(report);
        Assertions.AssertAllInputsCompleted(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SourceThatThrows_FailFast_AbandonsAcceptedItems()
    {
        var report = await Pipeline.From(Kit.ThrowingSource(5, new InvalidOperationException("broke")), new PipelineOptions { FailureMode = FailureMode.FailFast })
            .Stage("parked", async (int x, CancellationToken ct) => { await Task.Delay(Timeout.Infinite, ct); return x; },
                new StageOptions { Parallelism = 2, Capacity = 8 })
            .RunAsync(_ => { });

        Assert.Equal(RunOutcome.Faulted, report.Outcome);
        Assert.IsType<SourceFailedException>(report.FatalError);
        Assert.Equal(5, report.Accepted);
        Assert.Equal(5, report.NotProcessed);
        Assert.Equal(0, report.Delivered);
        Assertions.AssertBalanced(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SourceThatThrowsImmediately_ReportsFaultedWithNothingAccepted()
    {
        var report = await Pipeline.From(Kit.ThrowingSource(0, new ArgumentException("first pull")))
            .Stage("f", x => x).RunAsync(_ => { });
        Assert.Equal(RunOutcome.Faulted, report.Outcome);
        Assert.Equal(0, report.Accepted);
        Assertions.AssertBalanced(report);
        Assertions.AssertAllInputsCompleted(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SinkThatThrows_Continue_DeadLettersThoseItemsUnderTheSinkStage()
    {
        var good = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(100))
            .RunAsync((x, _) =>
            {
                if (x % 2 == 1) throw new IOException("sink rejects " + x);
                good.Add(x);
                return ValueTask.CompletedTask;
            });
        Assert.Equal(RunOutcome.Completed, report.Outcome);
        Assert.Equal(50, report.Delivered);
        Assert.Equal(50, report.DeadLettered);
        Assert.All(report.DeadLetters, d => { Assert.Equal("sink", d.Stage); Assert.IsType<IOException>(d.Exception); });
        Assert.Equal(Enumerable.Range(0, 100).Where(x => x % 2 == 0), good.Items);
        Assertions.AssertBalanced(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SinkThatThrows_FailFast_FaultsWithSinkAsTheFailingStage()
    {
        var report = await Pipeline.From(Kit.Range(1_000_000), new PipelineOptions { FailureMode = FailureMode.FailFast })
            .RunAsync((x, _) => x == 5 ? throw new IOException("no space") : ValueTask.CompletedTask);
        Assert.Equal(RunOutcome.Faulted, report.Outcome);
        var fatal = Assert.IsType<StageFailedException>(report.FatalError);
        Assert.Equal("sink", fatal.Stage);
        Assert.Equal(5, fatal.Sequence);
        Assert.Equal(5, report.Delivered);
        Assert.True(report.Accepted < 100);
        Assertions.AssertBalanced(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SinkRetry_CountsOneDeliveryPerItem_AndSeveralSideEffectAttempts()
    {
        var calls = new Counter();
        var options = new PipelineOptions { SinkOptions = new StageOptions { Retry = new RetryPolicy(3) } };
        var report = await Pipeline.From(Kit.Range(1), options)
            .RunAsync((x, _) => calls.Increment() < 3 ? throw new IOException("flaky sink") : ValueTask.CompletedTask);
        Assert.Equal(3, calls.Value);
        Assert.Equal(1, report.Delivered);
        Assert.Equal(0, report.DeadLettered);
        Assert.Equal(2, report.Stages[^1].Retried);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ThrowingSink_WithParallelSinkWorkers_StillBalances()
    {
        var options = new PipelineOptions { SinkOptions = new StageOptions { Parallelism = 4, Capacity = 2 } };
        var report = await Pipeline.From(Kit.Range(400), options)
            .RunAsync(async (x, _) => { await Task.Yield(); if (x % 3 == 0) throw new Exception("e"); });
        Assert.Equal(134, report.DeadLettered);
        Assert.Equal(266, report.Delivered);
        Assertions.AssertBalanced(report);
    }
}

public class StageExceptionTests
{
    private static readonly Func<Exception>[] Factories =
    [
        () => new InvalidOperationException("ioe"),
        () => new ArgumentNullException("p", "ane"),
        () => new NotSupportedException("nse"),
        () => new TimeoutException("te"),
        () => new IOException("io"),
        () => new CustomStageException("custom"),
        () => new OperationCanceledException("user-level cancel, not the run token"),
        () => new AggregateException(new Exception("inner")),
        () => new DivideByZeroException(),
    ];

    public class CustomStageException(string message) : Exception(message);

    [Theory(Timeout = Limits.Ms)]
    [InlineData(0)] [InlineData(1)] [InlineData(2)] [InlineData(3)] [InlineData(4)]
    [InlineData(5)] [InlineData(6)] [InlineData(7)] [InlineData(8)]
    public async Task DeadLetter_KeepsTheExactExceptionInstanceAndType(int which)
    {
        var thrown = Factories[which]();
        var report = await Pipeline.From(new[] { 1 })
            .Stage<int>("f", async (x, _) => { await Task.Yield(); throw thrown; })
            .RunAsync(_ => { });
        var dl = Assert.Single(report.DeadLetters);
        Assert.Same(thrown, dl.Exception);
        Assert.Equal(RunOutcome.Completed, report.Outcome);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task SynchronousThrow_BeforeAnyValueTaskExists_IsCaptured()
    {
        var report = await Pipeline.From(new[] { 1 })
            .Stage<int>("f", (x, _) => throw new NotSupportedException("sync"))
            .RunAsync(_ => { });
        Assert.IsType<NotSupportedException>(Assert.Single(report.DeadLetters).Exception);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task NullReference_FromRealCode_IsAnOrdinaryFailure()
    {
        var report = await Pipeline.From(new string?[] { "abc", null, "de" })
            .Stage("len", s => s!.Length)
            .RunAsync(_ => { });
        Assert.Equal(2, report.Delivered);
        Assert.IsType<NullReferenceException>(Assert.Single(report.DeadLetters).Exception);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task UserOperationCanceled_IsRetriedLikeAnyFailure_WhenTheRunTokenIsNotCancelled()
    {
        var calls = new Counter();
        var report = await Pipeline.From(new[] { 1 })
            .Stage<int>("f", (x, _) => { calls.Increment(); throw new OperationCanceledException(); },
                new StageOptions { Retry = new RetryPolicy(3) })
            .RunAsync(_ => { });
        Assert.Equal(3, calls.Value);
        Assert.Equal(RunOutcome.Completed, report.Outcome);
        Assert.Equal(1, report.DeadLettered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ThrowingShouldRetryCallback_IsAnInternalFatalError_NotASilentHang()
    {
        var policy = new RetryPolicy(3, shouldRetry: _ => throw new InvalidOperationException("bug in predicate"));
        var report = await Pipeline.From(Kit.Range(20))
            .Stage<int>("f", (x, _) => throw new Exception("e"), new StageOptions { Retry = policy })
            .RunAsync(_ => { });
        Assert.Equal(RunOutcome.Faulted, report.Outcome);
        var fatal = Assert.IsType<PipelineInternalException>(report.FatalError);
        Assert.Equal("f", fatal.Stage);
        Assertions.AssertBalanced(report);
        Assertions.AssertAllInputsCompleted(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task StageFailedException_CarriesStageSequenceAttemptsAndInner()
    {
        var inner = new FormatException("fmt");
        var report = await Pipeline.From(new[] { 10, 20 }, new PipelineOptions { FailureMode = FailureMode.FailFast })
            .Stage<int>("parse", (x, _) => x == 20 ? throw inner : new ValueTask<int>(x), new StageOptions { Retry = new RetryPolicy(4) })
            .RunAsync(_ => { });
        var fatal = Assert.IsType<StageFailedException>(report.FatalError);
        Assert.Equal(("parse", 1L, 4), (fatal.Stage, fatal.Sequence, fatal.Attempts));
        Assert.Same(inner, fatal.InnerException);
        Assert.Contains("parse", fatal.Message);
        Assert.Contains("4 attempt", fatal.Message);
    }

    [Fact(Timeout = Limits.Ms)]
    public void Cancelled_ThrowIfNotCompleted_ThrowsOperationCanceled()
    {
        var report = new RunReport
        {
            Outcome = RunOutcome.Cancelled, Accepted = 0, Delivered = 0, DeadLettered = 0,
            NotProcessedSequences = Array.Empty<long>(), DeadLetters = Array.Empty<DeadLetter>(),
            Stages = Array.Empty<StageMetrics>(), Elapsed = TimeSpan.Zero,
        };
        Assert.Throws<OperationCanceledException>(report.ThrowIfNotCompleted);
    }
}
