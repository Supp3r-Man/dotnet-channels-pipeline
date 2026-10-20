using System.Collections.Concurrent;

namespace ChannelsPipeline;

/// <summary>How a run ended.</summary>
public enum RunOutcome
{
    /// <summary>The source was exhausted and every accepted item was delivered or dead-lettered.</summary>
    Completed,

    /// <summary>The caller's token cancelled the run before it finished; see the report for what was not processed.</summary>
    Cancelled,

    /// <summary>A fatal error ended the run (fail-fast item failure, throwing source, throwing dead-letter sink).</summary>
    Faulted,
}

/// <summary>An item that exhausted its retries.</summary>
/// <param name="Sequence">Zero-based position of the item in the source (as accepted by the pipeline).</param>
/// <param name="Stage">Name of the stage (or "sink") that failed it.</param>
/// <param name="Item">The input the failing stage received.</param>
/// <param name="Attempts">How many times the stage function was invoked for this item.</param>
/// <param name="Exception">The exception of the last attempt.</param>
public sealed record DeadLetter(long Sequence, string Stage, object? Item, int Attempts, Exception Exception);

/// <summary>Receives dead letters. Calls are serialized by the pipeline.</summary>
public interface IDeadLetterSink
{
    ValueTask WriteAsync(DeadLetter deadLetter, CancellationToken cancellationToken);
}

/// <summary>A simple thread-safe in-memory <see cref="IDeadLetterSink"/>.</summary>
public sealed class InMemoryDeadLetterSink : IDeadLetterSink
{
    private readonly ConcurrentQueue<DeadLetter> _items = new();

    public IReadOnlyCollection<DeadLetter> Items => _items.ToArray();

    public ValueTask WriteAsync(DeadLetter deadLetter, CancellationToken cancellationToken)
    {
        _items.Enqueue(deadLetter);
        return ValueTask.CompletedTask;
    }
}

/// <summary>Metrics of one stage, either a live snapshot or the final values.</summary>
public sealed record StageMetrics
{
    public required string Name { get; init; }
    /// <summary>Items taken from the input queue.</summary>
    public long In { get; init; }
    /// <summary>Items successfully processed and handed downstream (for the sink: delivered).</summary>
    public long Out { get; init; }
    /// <summary>Items dead-lettered by this stage.</summary>
    public long Failed { get; init; }
    /// <summary>Extra attempts made (an item that succeeded on its 3rd try adds 2).</summary>
    public long Retried { get; init; }
    /// <summary>Items currently waiting in the input queue.</summary>
    public int QueueDepth { get; init; }
    /// <summary>Highest queue depth observed right after an enqueue. Never exceeds <see cref="QueueCapacity"/>.</summary>
    public int MaxQueueDepth { get; init; }
    public int QueueCapacity { get; init; }
    public int Parallelism { get; init; }
    /// <summary>Workers currently executing the stage function (including retries and backoff waits).</summary>
    public int BusyWorkers { get; init; }
    public int MaxBusyWorkers { get; init; }
    /// <summary>Wall time from stage start to stage end (or to now for a live snapshot).</summary>
    public TimeSpan TotalTime { get; init; }
    /// <summary>Sum over workers of time spent executing the stage function.</summary>
    public TimeSpan BusyTime { get; init; }
    /// <summary>Finished results currently held back to restore order (ordered stages with Parallelism &gt; 1).</summary>
    public int ReorderBuffered { get; init; }
    /// <summary>Highest number of results ever held back. Bounded by <c>ReorderWindow</c>.</summary>
    public int MaxReorderBuffered { get; init; }
    /// <summary>The reorder window in effect (0 when the stage does not reorder).</summary>
    public int ReorderWindow { get; init; }
    /// <summary>True once the upstream writer completed this stage's input queue (leak check).</summary>
    public bool InputCompleted { get; init; }
}

/// <summary>Live view of a running pipeline.</summary>
public sealed record RunSnapshot(long Accepted, long Delivered, long DeadLettered, IReadOnlyList<StageMetrics> Stages);

/// <summary>The final account of a run. Always produced, also after cancellation or a fatal error.</summary>
public sealed record RunReport
{
    public required RunOutcome Outcome { get; init; }

    /// <summary>The first fatal error, if any (<see cref="StageFailedException"/>, <see cref="SourceFailedException"/>, ...).</summary>
    public Exception? FatalError { get; init; }

    /// <summary>Items pulled from the source.</summary>
    public required long Accepted { get; init; }
    /// <summary>Items the sink function completed successfully.</summary>
    public required long Delivered { get; init; }
    /// <summary>Items that exhausted their retries and were written to the dead-letter sink.</summary>
    public required long DeadLettered { get; init; }
    /// <summary>Accepted items that were neither delivered nor dead-lettered (cancelled, abandoned by fail-fast, ...).</summary>
    public long NotProcessed => NotProcessedSequences.Count;
    /// <summary>Exactly which accepted items (source sequence numbers, ascending) were not processed.</summary>
    public required IReadOnlyList<long> NotProcessedSequences { get; init; }
    /// <summary>The first <see cref="PipelineOptions.MaxReportedDeadLetters"/> dead letters.</summary>
    public required IReadOnlyList<DeadLetter> DeadLetters { get; init; }
    public required IReadOnlyList<StageMetrics> Stages { get; init; }
    public required TimeSpan Elapsed { get; init; }

    /// <summary>The conservation law: every accepted item is in exactly one of delivered, dead-lettered, not-processed.</summary>
    public bool IsBalanced => Accepted == Delivered + DeadLettered + NotProcessed;

    /// <summary>Throws unless the run completed. Cancelled throws <see cref="OperationCanceledException"/>, Faulted <see cref="PipelineFailedException"/>.</summary>
    public void ThrowIfNotCompleted()
    {
        switch (Outcome)
        {
            case RunOutcome.Cancelled: throw new OperationCanceledException("The pipeline run was cancelled.");
            case RunOutcome.Faulted: throw new PipelineFailedException(this);
        }
    }
}

/// <summary>Thrown by <see cref="RunReport.ThrowIfNotCompleted"/> for a faulted run; <see cref="Exception.InnerException"/> is the fatal error.</summary>
public sealed class PipelineFailedException(RunReport report)
    : Exception("The pipeline run faulted: " + report.FatalError?.Message, report.FatalError)
{
    public RunReport Report { get; } = report;
}

/// <summary>Fatal error under <see cref="FailureMode.FailFast"/>: an item exhausted its retries.</summary>
public sealed class StageFailedException(string stage, long sequence, int attempts, Exception inner)
    : Exception($"Stage '{stage}' failed item #{sequence} after {attempts} attempt(s): {inner.Message}", inner)
{
    public string Stage { get; } = stage;
    public long Sequence { get; } = sequence;
    public int Attempts { get; } = attempts;
}

/// <summary>The source enumerable threw. <see cref="Exception.InnerException"/> is what it threw.</summary>
public sealed class SourceFailedException(Exception inner)
    : Exception("The pipeline source threw: " + inner.Message, inner);

/// <summary>The dead-letter sink threw, so accounting for the item could not be completed.</summary>
public sealed class DeadLetterSinkException(Exception inner)
    : Exception("The dead-letter sink threw: " + inner.Message, inner);

/// <summary>An attempt exceeded <see cref="StageOptions.ItemTimeout"/>.</summary>
public sealed class ItemTimeoutException(string stage, TimeSpan timeout)
    : TimeoutException($"Stage '{stage}': item attempt exceeded its {timeout} timeout.")
{
    public string Stage { get; } = stage;
    public TimeSpan Timeout { get; } = timeout;
}

/// <summary>An unexpected internal error (a bug in a retry-policy callback, for example) ended the run.</summary>
public sealed class PipelineInternalException(string stage, Exception inner)
    : Exception($"Unexpected error in '{stage}': {inner.Message}", inner)
{
    public string Stage { get; } = stage;
}
