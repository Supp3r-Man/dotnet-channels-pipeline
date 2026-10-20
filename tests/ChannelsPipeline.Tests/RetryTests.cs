namespace ChannelsPipeline.Tests;

public class RetryTests
{
    private static StageOptions Retrying(int attempts) => new() { Retry = new RetryPolicy(attempts) };

    [Fact(Timeout = Limits.Ms)]
    public async Task AlwaysFailing_ItemIsRetriedThenDeadLettered_WithExactAttemptCount()
    {
        var calls = new Counter();
        var sink = new Collector<int>();
        var report = await Pipeline.From(new[] { 41 })
            .Stage<int>("flaky", (x, _) => { calls.Increment(); throw new InvalidOperationException("nope"); }, Retrying(3))
            .RunAsync(sink.Sink);

        Assert.Equal(3, calls.Value);
        Assert.Equal(RunOutcome.Completed, report.Outcome);
        Assert.Equal(0, report.Delivered);
        Assert.Equal(1, report.DeadLettered);
        var dl = Assert.Single(report.DeadLetters);
        Assert.Equal(3, dl.Attempts);
        Assert.Equal("flaky", dl.Stage);
        Assert.Equal(41, dl.Item);
        Assert.Equal(0, dl.Sequence);
        Assert.IsType<InvalidOperationException>(dl.Exception);
        var m = report.Stages[0];
        Assert.Equal((1, 0, 1, 2), ((int)m.In, (int)m.Out, (int)m.Failed, (int)m.Retried));
        Assertions.AssertBalanced(report);
    }

    [Theory(Timeout = Limits.Ms)]
    [InlineData(1)] [InlineData(2)] [InlineData(3)] [InlineData(4)] [InlineData(5)] [InlineData(6)]
    public async Task MaxAttempts_IsExactlyTheNumberOfInvocations(int maxAttempts)
    {
        var calls = new Counter();
        var report = await Pipeline.From(new[] { 1 })
            .Stage<int>("f", (x, _) => { calls.Increment(); throw new Exception("x"); }, Retrying(maxAttempts))
            .RunAsync(_ => { });
        Assert.Equal(maxAttempts, calls.Value);
        Assert.Equal(maxAttempts, report.DeadLetters[0].Attempts);
        Assert.Equal(maxAttempts - 1, report.Stages[0].Retried);
    }

    [Theory(Timeout = Limits.Ms)]
    [InlineData(1)] [InlineData(2)] [InlineData(3)] [InlineData(4)]
    public async Task SucceedingOnTheNthAttempt_Delivers_AndCountsRetries(int succeedOn)
    {
        var calls = new Counter();
        var sink = new Collector<int>();
        var report = await Pipeline.From(new[] { 7 })
            .Stage("eventually", async (int x, CancellationToken _) =>
            {
                await Task.Yield();
                if (calls.Increment() < succeedOn) throw new InvalidOperationException("transient");
                return x * 2;
            }, Retrying(4))
            .RunAsync(sink.Sink);

        Assert.Equal(new[] { 14 }, sink.Items);
        Assert.Equal(succeedOn, calls.Value);
        Assert.Equal(succeedOn - 1, report.Stages[0].Retried);
        Assert.Equal(0, report.DeadLettered);
        Assert.Equal(0, report.Stages[0].Failed);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task NoRetryByDefault_OneAttemptThenDeadLetter()
    {
        var calls = new Counter();
        var report = await Pipeline.From(new[] { 1 })
            .Stage<int>("f", (x, _) => { calls.Increment(); throw new Exception("x"); })
            .RunAsync(_ => { });
        Assert.Equal(1, calls.Value);
        Assert.Equal(1, report.DeadLetters[0].Attempts);
        Assert.Equal(0, report.Stages[0].Retried);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ShouldRetry_False_StopsAfterFirstAttempt()
    {
        var calls = new Counter();
        var policy = new RetryPolicy(5, shouldRetry: _ => false);
        var report = await Pipeline.From(new[] { 1 })
            .Stage<int>("f", (x, _) => { calls.Increment(); throw new Exception("x"); }, new StageOptions { Retry = policy })
            .RunAsync(_ => { });
        Assert.Equal(1, calls.Value);
        Assert.Equal(1, report.DeadLetters[0].Attempts);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ShouldRetry_SeparatesTransientFromPermanentFailures()
    {
        var calls = new System.Collections.Concurrent.ConcurrentDictionary<int, int>();
        var policy = new RetryPolicy(3, shouldRetry: ex => ex is IOException);
        var report = await Pipeline.From(new[] { 0, 1 })
            .Stage<int>("f", (x, _) =>
            {
                calls.AddOrUpdate(x, 1, (_, n) => n + 1);
                throw x == 0 ? new IOException("transient") : new ArgumentException("permanent");
            }, new StageOptions { Retry = policy })
            .RunAsync(_ => { });
        Assert.Equal(3, calls[0]);
        Assert.Equal(1, calls[1]);
        Assert.Equal(3, report.DeadLetters.Single(d => d.Sequence == 0).Attempts);
        Assert.Equal(1, report.DeadLetters.Single(d => d.Sequence == 1).Attempts);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ManyItems_RetryIndependently_InParallel()
    {
        var attempts = new System.Collections.Concurrent.ConcurrentDictionary<int, int>();
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(200))
            .Stage("f", async (int x, CancellationToken _) =>
            {
                await Task.Yield();
                var n = attempts.AddOrUpdate(x, 1, (_, c) => c + 1);
                if (x % 2 == 0 && n < 3) throw new InvalidOperationException();
                return x;
            }, new StageOptions { Parallelism = 6, Retry = new RetryPolicy(3), Capacity = 4 })
            .RunAsync(sink.Sink);
        Assert.Equal(200, report.Delivered);
        Assert.Equal(Enumerable.Range(0, 200), sink.Items);
        Assert.Equal(100 * 2, report.Stages[0].Retried);
        Assert.Equal(0, report.DeadLettered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task Backoff_WaitsOnTheInjectedClock_BeforeEachRetry()
    {
        var clock = new TestClock();
        var attempts = new Signal();
        var options = new PipelineOptions { TimeProvider = clock };
        var run = Pipeline.From(new[] { 1 }, options)
            .Stage<int>("f", (x, _) => { attempts.Increment(); throw new Exception("x"); },
                new StageOptions { Retry = RetryPolicy.Fixed(3, TimeSpan.FromSeconds(10)) })
            .Start((_, _) => ValueTask.CompletedTask);

        await attempts.AtLeastAsync(1);
        await clock.TimersCreated.AtLeastAsync(1);
        clock.Advance(TimeSpan.FromSeconds(9));
        Assert.Equal(1, attempts.Count);               // still backing off
        clock.Advance(TimeSpan.FromSeconds(1));
        await attempts.AtLeastAsync(2);
        await clock.TimersCreated.AtLeastAsync(2);
        clock.Advance(TimeSpan.FromSeconds(9));
        Assert.Equal(2, attempts.Count);
        clock.Advance(TimeSpan.FromSeconds(1));

        var report = await run.Completion;
        Assert.Equal(3, attempts.Count);
        Assert.Equal(3, report.DeadLetters[0].Attempts);
        Assert.Equal(TimeSpan.FromSeconds(20), report.Elapsed);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ExponentialBackoff_DoublesTheWaitBetweenAttempts()
    {
        var clock = new TestClock();
        var started = new List<TimeSpan>();
        var t0 = clock.GetUtcNow();
        var attempts = new Signal();
        var run = Pipeline.From(new[] { 1 }, new PipelineOptions { TimeProvider = clock })
            .Stage<int>("f", (x, _) =>
            {
                lock (started) started.Add(clock.GetUtcNow() - t0);
                attempts.Increment();
                throw new Exception("x");
            }, new StageOptions { Retry = RetryPolicy.Exponential(4, TimeSpan.FromSeconds(1)) })
            .Start((_, _) => ValueTask.CompletedTask);

        foreach (var delay in new[] { 1, 2, 4 })
        {
            await clock.TimersCreated.AtLeastAsync(delay switch { 1 => 1, 2 => 2, _ => 3 });
            clock.Advance(TimeSpan.FromSeconds(delay));
        }

        await run.Completion;
        Assert.Equal(new[] { 0, 1, 3, 7 }.Select(s => TimeSpan.FromSeconds(s)), started);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task DeadLetter_InLaterStage_RecordsThatStagesInput()
    {
        var report = await Pipeline.From(new[] { 5 })
            .Stage("first", x => x * 100)
            .Stage<string>("second", (x, _) => throw new FormatException("bad " + x), Retrying(2))
            .RunAsync(_ => { });
        var dl = Assert.Single(report.DeadLetters);
        Assert.Equal("second", dl.Stage);
        Assert.Equal(500, dl.Item);
        Assert.Equal(2, dl.Attempts);
        Assert.Equal("bad 500", dl.Exception.Message);
        Assert.Equal(1, report.Stages[1].Failed);
        Assert.Equal(0, report.Stages[0].Failed);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ExternalDeadLetterSink_ReceivesEverything_ReportIsCapped_CountIsExact()
    {
        var external = new InMemoryDeadLetterSink();
        var options = new PipelineOptions { DeadLetterSink = external, MaxReportedDeadLetters = 10 };
        var report = await Pipeline.From(Kit.Range(100), options)
            .Stage("f", async (int x, CancellationToken _) => { await Task.Yield(); if (x % 2 == 0) throw new Exception("e"); return x; },
                new StageOptions { Parallelism = 4 })
            .RunAsync(_ => { });
        Assert.Equal(50, report.DeadLettered);
        Assert.Equal(10, report.DeadLetters.Count);
        Assert.Equal(50, external.Items.Count);
        Assert.Equal(50, external.Items.Select(d => d.Sequence).Distinct().Count());
        Assertions.AssertBalanced(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task DeadLetterWrites_AreSerialized()
    {
        var active = new Counter();
        var overlap = new Flag();
        var sink = new DelegateDeadLetterSink(async (dl, ct) =>
        {
            if (active.Increment() > 1) overlap.Set();
            await Task.Yield();
            await Task.Yield();
            active.Decrement();
        });
        var report = await Pipeline.From(Kit.Range(200), new PipelineOptions { DeadLetterSink = sink })
            .Stage<int>("f", (x, _) => throw new Exception("e"), new StageOptions { Parallelism = 8 })
            .RunAsync(_ => { });
        Assert.Equal(200, report.DeadLettered);
        Assert.False(overlap.IsSet);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ThrowingDeadLetterSink_IsFatal_AndTheItemIsNotCountedAsDeadLettered()
    {
        var sink = new DelegateDeadLetterSink((dl, ct) => throw new IOException("disk full"));
        var report = await Pipeline.From(Kit.Range(10), new PipelineOptions { DeadLetterSink = sink })
            .Stage<int>("f", (x, _) => throw new Exception("e"))
            .RunAsync(_ => { });
        Assert.Equal(RunOutcome.Faulted, report.Outcome);
        var fatal = Assert.IsType<DeadLetterSinkException>(report.FatalError);
        Assert.IsType<IOException>(fatal.InnerException);
        Assert.Equal(0, report.DeadLettered);
        Assert.True(report.NotProcessed >= 1);
        Assertions.AssertBalanced(report);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ItemTimeout_FailsTheAttemptWithItemTimeoutException()
    {
        var clock = new TestClock();
        var entered = new Signal();
        var run = Pipeline.From(new[] { 1 }, new PipelineOptions { TimeProvider = clock })
            .Stage("slow", async (int x, CancellationToken ct) =>
            {
                entered.Increment();
                await Task.Delay(Timeout.Infinite, ct);
                return x;
            }, new StageOptions { ItemTimeout = TimeSpan.FromSeconds(5) })
            .Start((_, _) => ValueTask.CompletedTask);

        await entered.AtLeastAsync(1);
        clock.Advance(TimeSpan.FromSeconds(4));
        Assert.Equal(1, entered.Count);
        clock.Advance(TimeSpan.FromSeconds(1));
        var report = await run.Completion;

        var dl = Assert.Single(report.DeadLetters);
        var ex = Assert.IsType<ItemTimeoutException>(dl.Exception);
        Assert.IsAssignableFrom<TimeoutException>(ex);
        Assert.Equal("slow", ex.Stage);
        Assert.Equal(TimeSpan.FromSeconds(5), ex.Timeout);
        Assert.Equal(RunOutcome.Completed, report.Outcome);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ItemTimeout_ThenRetrySucceeds()
    {
        var clock = new TestClock();
        var entered = new Signal();
        var sink = new Collector<int>();
        var run = Pipeline.From(new[] { 9 }, new PipelineOptions { TimeProvider = clock })
            .Stage("slow-then-fast", async (int x, CancellationToken ct) =>
            {
                if (entered.Increment() == 1) await Task.Delay(Timeout.Infinite, ct);
                return x;
            }, new StageOptions { ItemTimeout = TimeSpan.FromSeconds(2), Retry = new RetryPolicy(2) })
            .Start(sink.Sink);

        await entered.AtLeastAsync(1);
        clock.Advance(TimeSpan.FromSeconds(2));
        var report = await run.Completion;
        Assert.Equal(new[] { 9 }, sink.Items);
        Assert.Equal(1, report.Stages[0].Retried);
        Assert.Equal(0, report.DeadLettered);
    }

    [Fact(Timeout = Limits.Ms)]
    public async Task ItemTimeout_DoesNotFire_WhenTheItemFinishesInTime()
    {
        var clock = new TestClock();
        var sink = new Collector<int>();
        var report = await Pipeline.From(Kit.Range(20), new PipelineOptions { TimeProvider = clock })
            .Stage("fast", x => x, new StageOptions { ItemTimeout = TimeSpan.FromSeconds(1) })
            .RunAsync(sink.Sink);
        Assert.Equal(20, report.Delivered);
        Assert.Equal(0, report.DeadLettered);
    }
}

public sealed class DelegateDeadLetterSink(Func<DeadLetter, CancellationToken, ValueTask> write) : IDeadLetterSink
{
    public ValueTask WriteAsync(DeadLetter deadLetter, CancellationToken cancellationToken) => write(deadLetter, cancellationToken);
}
