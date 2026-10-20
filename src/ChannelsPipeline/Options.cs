namespace ChannelsPipeline;

/// <summary>What the pipeline does when an item exhausts its retries (or another fatal error occurs).</summary>
public enum FailureMode
{
    /// <summary>
    /// Dead-letter the failed item and keep going. Only infrastructure failures (a throwing dead-letter sink)
    /// and, after draining, a throwing source, make the run <see cref="RunOutcome.Faulted"/>.
    /// </summary>
    Continue = 0,

    /// <summary>
    /// The first failure that survives its retry policy is recorded, dead-lettered, and then cancels the whole run
    /// immediately. Everything not yet delivered or dead-lettered is reported as not processed.
    /// </summary>
    FailFast = 1,
}

/// <summary>Per-item retry policy. <see cref="MaxAttempts"/> counts the first try, so 1 means "no retry".</summary>
public sealed class RetryPolicy
{
    /// <summary>One attempt, no retry.</summary>
    public static RetryPolicy None { get; } = new(1);

    /// <summary>Total attempts including the first (&gt;= 1).</summary>
    public int MaxAttempts { get; }

    /// <summary>Delay before the next attempt, given the 1-based number of the attempt that just failed.</summary>
    public Func<int, TimeSpan> Backoff { get; }

    /// <summary>Whether an exception is worth retrying. Defaults to "yes, always".</summary>
    public Func<Exception, bool> ShouldRetry { get; }

    public RetryPolicy(int maxAttempts, Func<int, TimeSpan>? backoff = null, Func<Exception, bool>? shouldRetry = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        MaxAttempts = maxAttempts;
        Backoff = backoff ?? (_ => TimeSpan.Zero);
        ShouldRetry = shouldRetry ?? (_ => true);
    }

    /// <summary>The same delay between every attempt.</summary>
    public static RetryPolicy Fixed(int maxAttempts, TimeSpan delay) => new(maxAttempts, _ => delay);

    /// <summary>Delay doubles after each failed attempt (base, 2*base, 4*base, ...), optionally capped.</summary>
    public static RetryPolicy Exponential(int maxAttempts, TimeSpan baseDelay, TimeSpan? maxDelay = null) =>
        new(maxAttempts, failedAttempt =>
        {
            var factor = Math.Pow(2, Math.Min(failedAttempt - 1, 30));
            var delay = TimeSpan.FromTicks((long)Math.Min(baseDelay.Ticks * factor, long.MaxValue / 2.0));
            return maxDelay is { } cap && delay > cap ? cap : delay;
        });
}

/// <summary>Configuration of one stage.</summary>
public sealed record StageOptions
{
    /// <summary>
    /// Capacity of this stage's input queue (bounded; a full queue blocks whoever feeds it). Must be &gt;= 1.
    /// This is the backpressure knob.
    /// </summary>
    public int Capacity { get; init; } = 16;

    /// <summary>Number of concurrent workers (&gt;= 1).</summary>
    public int Parallelism { get; init; } = 1;

    /// <summary>
    /// If true (default) results leave the stage in the order items entered it. With <see cref="Parallelism"/> &gt; 1
    /// this needs a reorder buffer, whose size is bounded by <see cref="ReorderWindow"/>.
    /// </summary>
    public bool Ordered { get; init; } = true;

    /// <summary>
    /// Ordered stages only: the stage never holds more than this many items (in progress + waiting + being emitted),
    /// so the reorder buffer never holds more than <c>ReorderWindow</c> finished results (at most <c>ReorderWindow - 1</c> while the oldest unfinished item is still running). Must be &gt;= Parallelism.
    /// Null means <c>4 * Parallelism</c>.
    /// </summary>
    public int? ReorderWindow { get; init; }

    /// <summary>Per-item retry policy. Default: no retry.</summary>
    public RetryPolicy Retry { get; init; } = RetryPolicy.None;

    /// <summary>
    /// Optional cooperative per-attempt timeout. The token handed to the stage function is cancelled after this long;
    /// a function that ignores its token is not interrupted. A timed-out attempt fails with <see cref="ItemTimeoutException"/>.
    /// </summary>
    public TimeSpan? ItemTimeout { get; init; }

    internal int EffectiveReorderWindow => ReorderWindow ?? checked(4 * Parallelism);

    /// <summary>True when the reorder machinery is needed (ordered output with more than one worker).</summary>
    internal bool NeedsReordering => Ordered && Parallelism > 1;

    internal void Validate(string stage)
    {
        if (Capacity < 1) throw new ArgumentOutOfRangeException(nameof(Capacity), Capacity, $"Stage '{stage}': capacity must be >= 1.");
        if (Parallelism < 1) throw new ArgumentOutOfRangeException(nameof(Parallelism), Parallelism, $"Stage '{stage}': parallelism must be >= 1.");
        if (ReorderWindow is { } w && w < Parallelism)
            throw new ArgumentOutOfRangeException(nameof(ReorderWindow), w, $"Stage '{stage}': reorder window must be >= parallelism.");
        if (ItemTimeout is { } t && t <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ItemTimeout), t, $"Stage '{stage}': item timeout must be positive.");
        ArgumentNullException.ThrowIfNull(Retry);
    }
}

/// <summary>Pipeline-wide configuration.</summary>
public sealed record PipelineOptions
{
    /// <summary>Fail-fast or continue. Default: <see cref="FailureMode.Continue"/>.</summary>
    public FailureMode FailureMode { get; init; } = FailureMode.Continue;

    /// <summary>Optional external dead-letter sink. Writes are serialized, so the sink need not be thread-safe.</summary>
    public IDeadLetterSink? DeadLetterSink { get; init; }

    /// <summary>The report keeps at most this many dead letters (the count is always exact). Default 1000.</summary>
    public int MaxReportedDeadLetters { get; init; } = 1000;

    /// <summary>Options of the final "sink" stage (input queue capacity, parallelism, ordering, retry, timeout).</summary>
    public StageOptions SinkOptions { get; init; } = new();

    /// <summary>Clock used for retry backoff, item timeouts and stage timings. Inject a fake in tests.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
}
