using System.Runtime.CompilerServices;

namespace ChannelsPipeline;

/// <summary>Entry point: <c>Pipeline.From(source).Stage(...).Stage(...).RunAsync(sink, ct)</c>.</summary>
public static class Pipeline
{
    /// <summary>Starts a pipeline from an async sequence. The pipeline pulls from it only as fast as the first stage accepts items.</summary>
    public static PipelineBuilder<T> From<T>(IAsyncEnumerable<T> source, PipelineOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        options ??= new PipelineOptions();
        ValidateOptions(options);
        return new PipelineBuilder<T>(options, (ctx, downstream) => ctx.StartSource = () => PumpAsync(ctx, source, downstream), Array.Empty<string>());
    }

    /// <summary>Starts a pipeline from a synchronous sequence.</summary>
    public static PipelineBuilder<T> From<T>(IEnumerable<T> source, PipelineOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        return From(Iterate(source), options);
    }

    private static async IAsyncEnumerable<T> Iterate<T>(IEnumerable<T> source, [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var item in source)
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    private static void ValidateOptions(PipelineOptions options)
    {
        ArgumentNullException.ThrowIfNull(options.TimeProvider);
        ArgumentNullException.ThrowIfNull(options.SinkOptions);
        ArgumentOutOfRangeException.ThrowIfNegative(options.MaxReportedDeadLetters);
        options.SinkOptions.Validate("sink");
    }

    /// <summary>The source pump: the only place items enter. It blocks on a full first queue, which is the backpressure root.</summary>
    private static async Task PumpAsync<T>(RunContext ctx, IAsyncEnumerable<T> source, InputQueue<T> output)
    {
        var token = ctx.Token;
        try
        {
            await foreach (var item in source.WithCancellation(token).ConfigureAwait(false))
            {
                var seq = ctx.Accept();
                await output.WriteAsync(new Envelope<T>(seq, item), token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            ctx.MarkAborted();
        }
        catch (Exception ex)
        {
            // Fail-fast stops everything; otherwise stop intake and let accepted items drain.
            ctx.Fail(new SourceFailedException(ex), cancel: ctx.FailFast);
        }
        finally
        {
            output.Complete();
        }
    }
}

/// <summary>An immutable pipeline description whose output type is <typeparamref name="T"/>. Reusable: each Start wires fresh queues.</summary>
public sealed class PipelineBuilder<T>
{
    private readonly PipelineOptions _options;
    private readonly Action<RunContext, InputQueue<T>> _wire;
    private readonly IReadOnlyList<string> _names;

    internal PipelineBuilder(PipelineOptions options, Action<RunContext, InputQueue<T>> wire, IReadOnlyList<string> names)
    {
        _options = options;
        _wire = wire;
        _names = names;
    }

    /// <summary>Names of the stages added so far, in order.</summary>
    public IReadOnlyList<string> StageNames => _names;

    /// <summary>Adds a stage. Every input item produces exactly one output item or one dead letter.</summary>
    public PipelineBuilder<TOut> Stage<TOut>(string name, Func<T, CancellationToken, ValueTask<TOut>> function, StageOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(function);
        if (name == "sink" || _names.Contains(name)) throw new ArgumentException($"Stage name '{name}' is already used (\"sink\" is reserved).", nameof(name));
        options ??= new StageOptions();
        options.Validate(name);

        var upstream = _wire;
        return new PipelineBuilder<TOut>(_options, (ctx, downstream) =>
        {
            var input = new InputQueue<T>(options.Capacity);
            ctx.Stages.Add(new StageRunner<T, TOut>(name, function, options, input, downstream, ctx));
            upstream(ctx, input);
        }, _names.Append(name).ToArray());
    }

    /// <summary>Adds a synchronous stage.</summary>
    public PipelineBuilder<TOut> Stage<TOut>(string name, Func<T, TOut> function, StageOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(function);
        return Stage(name, (T x, CancellationToken _) => new ValueTask<TOut>(function(x)), options);
    }

    /// <summary>Starts the pipeline and returns immediately with a handle for live metrics and the final report.</summary>
    public PipelineRun Start(Func<T, CancellationToken, ValueTask> sink, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sink);
        var ctx = new RunContext(_options, cancellationToken);
        var sinkInput = new InputQueue<T>(_options.SinkOptions.Capacity);
        ctx.Stages.Add(new StageRunner<T, Unit>("sink", async (x, ct) =>
        {
            await sink(x, ct).ConfigureAwait(false);
            return default;
        }, _options.SinkOptions with { Ordered = false }, sinkInput, null, ctx));   // nothing downstream of the sink to keep in order
        _wire(ctx, sinkInput);
        ctx.Stages.Reverse();   // wiring walks from the sink back to the source

        foreach (var stage in ctx.Stages) stage.Start();
        var pump = Task.Run(ctx.StartSource!);
        return new PipelineRun(ctx, FinishAsync(ctx, pump));
    }

    /// <summary>Runs to completion, cancellation or fatal error and returns the report. Item failures never throw; see <see cref="RunReport"/>.</summary>
    public Task<RunReport> RunAsync(Func<T, CancellationToken, ValueTask> sink, CancellationToken cancellationToken = default) =>
        Start(sink, cancellationToken).Completion;

    /// <summary>Synchronous-sink overload.</summary>
    public Task<RunReport> RunAsync(Action<T> sink, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sink);
        return RunAsync((x, _) =>
        {
            sink(x);
            return ValueTask.CompletedTask;
        }, cancellationToken);
    }

    private static async Task<RunReport> FinishAsync(RunContext ctx, Task pump)
    {
        var started = ctx.Time.GetTimestamp();
        try
        {
            await Task.WhenAll(ctx.Stages.Select(s => s.Completion).Append(pump)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            ctx.Fail(new PipelineInternalException("run", ex), cancel: true);
        }

        var outcome = ctx.Fatal is not null ? RunOutcome.Faulted : ctx.Aborted ? RunOutcome.Cancelled : RunOutcome.Completed;
        var report = new RunReport
        {
            Outcome = outcome,
            FatalError = ctx.Fatal,
            Accepted = ctx.Accepted,
            Delivered = ctx.Delivered,
            DeadLettered = ctx.DeadLettered,
            NotProcessedSequences = ctx.PendingSequences(),
            DeadLetters = ctx.DeadLettersSnapshot(),
            Stages = ctx.Stages.Select(s => s.Snapshot()).ToArray(),
            Elapsed = ctx.Time.GetElapsedTime(started),
        };
        ctx.Dispose();
        return report;
    }
}

/// <summary>Handle to a started pipeline.</summary>
public sealed class PipelineRun
{
    private readonly RunContext _ctx;

    internal PipelineRun(RunContext ctx, Task<RunReport> completion)
    {
        _ctx = ctx;
        Completion = completion;
    }

    /// <summary>Completes when every stage worker and the source pump have stopped. Never faults for item failures.</summary>
    public Task<RunReport> Completion { get; }

    /// <summary>A consistent-enough live view: per-counter reads are atomic, the set of counters is not read atomically.</summary>
    public RunSnapshot Snapshot() => _ctx.Snapshot();
}
