namespace ChannelsPipeline;

internal interface IStageRunner
{
    string Name { get; }
    Task Completion { get; }
    void Start();
    StageMetrics Snapshot();
}

internal readonly record struct Unit;

/// <summary>
/// Runs one stage: <c>Parallelism</c> workers pull from the bounded input queue, apply the function with retry and
/// timeout, and hand results to the next stage's queue (or, for the sink, mark them delivered).
/// </summary>
internal sealed class StageRunner<TIn, TOut> : IStageRunner
{
    private readonly string _name;
    private readonly Func<TIn, CancellationToken, ValueTask<TOut>> _fn;
    private readonly StageOptions _options;
    private readonly InputQueue<TIn> _input;
    private readonly InputQueue<TOut>? _output;   // null for the sink
    private readonly RunContext _ctx;

    private long _in, _out, _failed, _retried, _busyTicks;
    private int _busy, _maxBusy;
    private long _startTs, _endTs;
    private int _ended;   // 1 once _endTs is valid (a timestamp of 0 is a legal value, so it cannot be the sentinel)
    private Task _completion = Task.CompletedTask;

    // Ordered mode with Parallelism > 1.
    private readonly SemaphoreSlim? _window;
    private readonly object _takeLock = new();
    private long _nextOrdinal;
    private readonly object _orderLock = new();
    private readonly Dictionary<long, Slot> _buffer = new();
    private long _nextEmit;
    private bool _draining;
    private int _maxBuffered;

    private readonly record struct Slot(long Seq, bool HasValue, TOut? Value);

    public StageRunner(string name, Func<TIn, CancellationToken, ValueTask<TOut>> fn, StageOptions options,
        InputQueue<TIn> input, InputQueue<TOut>? output, RunContext ctx)
    {
        _name = name;
        _fn = fn;
        _options = options;
        _input = input;
        _output = output;
        _ctx = ctx;
        if (options.NeedsReordering) _window = new SemaphoreSlim(options.EffectiveReorderWindow, options.EffectiveReorderWindow);
    }

    public string Name => _name;
    public Task Completion => _completion;

    public void Start()
    {
        _startTs = _ctx.Time.GetTimestamp();
        var workers = new Task[_options.Parallelism];
        for (var i = 0; i < workers.Length; i++)
            workers[i] = Task.Run(_options.NeedsReordering ? OrderedWorkerAsync : SimpleWorkerAsync);
        _completion = FinishAsync(workers);
    }

    private async Task FinishAsync(Task[] workers)
    {
        try
        {
            await Task.WhenAll(workers).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref _endTs, _ctx.Time.GetTimestamp());
            Volatile.Write(ref _ended, 1);
            _output?.Complete();
        }
    }

    // ---------------------------------------------------------------- workers

    private async Task SimpleWorkerAsync()
    {
        var token = _ctx.Token;
        try
        {
            while (await _input.Reader.WaitToReadAsync(token).ConfigureAwait(false))
            {
                if (!_input.Reader.TryRead(out var env)) continue;
                Interlocked.Increment(ref _in);
                var (ok, value) = await ProcessAsync(env).ConfigureAwait(false);
                if (ok) await EmitAsync(env.Seq, value!).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            _ctx.MarkAborted();
        }
        catch (Exception ex)
        {
            _ctx.Fail(new PipelineInternalException(_name, ex), cancel: true);
        }
    }

    private async Task OrderedWorkerAsync()
    {
        var token = _ctx.Token;
        try
        {
            while (true)
            {
                // A permit is "one item inside the stage". Acquiring it before taking an item is what bounds the reorder buffer.
                await _window!.WaitAsync(token).ConfigureAwait(false);
                Envelope<TIn> env = default;
                long ordinal = 0;
                var taken = false;
                while (!taken && await _input.Reader.WaitToReadAsync(token).ConfigureAwait(false))
                {
                    lock (_takeLock)
                    {
                        // Read + ordinal assignment are atomic, so ordinals follow the FIFO order of the input queue.
                        if (_input.Reader.TryRead(out env))
                        {
                            ordinal = _nextOrdinal++;
                            taken = true;
                        }
                    }
                }

                if (!taken)
                {
                    _window.Release();
                    break;
                }

                Interlocked.Increment(ref _in);
                var (ok, value) = await ProcessAsync(env).ConfigureAwait(false);
                await CompleteOrderedAsync(ordinal, new Slot(env.Seq, ok, value)).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            _ctx.MarkAborted();
        }
        catch (Exception ex)
        {
            _ctx.Fail(new PipelineInternalException(_name, ex), cancel: true);
        }
    }

    /// <summary>Stores a finished result and, if nobody is draining yet, emits everything that is now in order.</summary>
    private async ValueTask CompleteOrderedAsync(long ordinal, Slot slot)
    {
        bool becomeDrainer;
        lock (_orderLock)
        {
            _buffer[ordinal] = slot;
            becomeDrainer = !_draining && _buffer.ContainsKey(_nextEmit);
            if (becomeDrainer) _draining = true;
            // The head result is about to be emitted by us, so it is not "waiting for an earlier item".
            var waiting = _buffer.Count - (becomeDrainer ? 1 : 0);
            if (waiting > _maxBuffered) _maxBuffered = waiting;
        }

        if (!becomeDrainer) return;

        while (true)
        {
            Slot next;
            lock (_orderLock)
            {
                if (!_buffer.Remove(_nextEmit, out next))
                {
                    _draining = false;
                    return;
                }

                _nextEmit++;
            }

            if (next.HasValue) await EmitAsync(next.Seq, next.Value!).ConfigureAwait(false);
            _window!.Release();
        }
    }

    private async ValueTask EmitAsync(long seq, TOut value)
    {
        if (_output is null)
        {
            // Sink: no await between "function returned" and "counted", so cancellation cannot split the two.
            _ctx.MarkDelivered(seq);
            Interlocked.Increment(ref _out);
            return;
        }

        await _output.WriteAsync(new Envelope<TOut>(seq, value), _ctx.Token).ConfigureAwait(false);
        Interlocked.Increment(ref _out);
    }

    // ---------------------------------------------------------------- one item: retry, timeout, dead letter

    private async ValueTask<(bool Ok, TOut? Value)> ProcessAsync(Envelope<TIn> env)
    {
        var token = _ctx.Token;
        var busy = Interlocked.Increment(ref _busy);
        int seen;
        while (busy > (seen = Volatile.Read(ref _maxBusy)))
        {
            if (Interlocked.CompareExchange(ref _maxBusy, busy, seen) == seen) break;
        }

        var started = _ctx.Time.GetTimestamp();
        try
        {
            var retry = _options.Retry;
            var attempt = 0;
            while (true)
            {
                attempt++;
                token.ThrowIfCancellationRequested();
                Exception failure;
                try
                {
                    return (true, await InvokeAsync(env.Value, token).ConfigureAwait(false));
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    failure = ex;
                }

                // If the run was cancelled meanwhile, abandon the item instead of recording a failure.
                token.ThrowIfCancellationRequested();

                if (attempt < retry.MaxAttempts && retry.ShouldRetry(failure))
                {
                    Interlocked.Increment(ref _retried);
                    var delay = retry.Backoff(attempt);
                    if (delay > TimeSpan.Zero) await Task.Delay(delay, _ctx.Time, token).ConfigureAwait(false);
                    continue;
                }

                await _ctx.DeadLetterAsync(new DeadLetter(env.Seq, _name, env.Value, attempt, failure), token).ConfigureAwait(false);
                Interlocked.Increment(ref _failed);
                if (_ctx.FailFast) _ctx.Fail(new StageFailedException(_name, env.Seq, attempt, failure), cancel: true);
                return (false, default);
            }
        }
        finally
        {
            Interlocked.Decrement(ref _busy);
            Interlocked.Add(ref _busyTicks, _ctx.Time.GetElapsedTime(started).Ticks);
        }
    }

    private async ValueTask<TOut> InvokeAsync(TIn value, CancellationToken token)
    {
        if (_options.ItemTimeout is not { } timeout) return await _fn(value, token).ConfigureAwait(false);

        using var timeoutCts = new CancellationTokenSource(timeout, _ctx.Time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(token, timeoutCts.Token);
        try
        {
            return await _fn(value, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !token.IsCancellationRequested)
        {
            throw new ItemTimeoutException(_name, timeout);
        }
    }

    // ---------------------------------------------------------------- metrics

    public StageMetrics Snapshot()
    {
        int buffered;
        lock (_orderLock) { buffered = _buffer.Count; }
        var total = Volatile.Read(ref _ended) == 1
            ? _ctx.Time.GetElapsedTime(_startTs, Volatile.Read(ref _endTs))
            : _ctx.Time.GetElapsedTime(_startTs);   // still running: a live value
        int maxBuffered;
        lock (_orderLock) { maxBuffered = _maxBuffered; }
        return new StageMetrics
        {
            Name = _name,
            In = Interlocked.Read(ref _in),
            Out = Interlocked.Read(ref _out),
            Failed = Interlocked.Read(ref _failed),
            Retried = Interlocked.Read(ref _retried),
            QueueDepth = _input.Depth,
            MaxQueueDepth = _input.MaxDepth,
            QueueCapacity = _input.Capacity,
            Parallelism = _options.Parallelism,
            BusyWorkers = Volatile.Read(ref _busy),
            MaxBusyWorkers = Volatile.Read(ref _maxBusy),
            TotalTime = total,
            BusyTime = TimeSpan.FromTicks(Interlocked.Read(ref _busyTicks)),
            ReorderBuffered = buffered,
            MaxReorderBuffered = maxBuffered,
            ReorderWindow = _options.NeedsReordering ? _options.EffectiveReorderWindow : 0,
            InputCompleted = _input.WriterCompleted,
        };
    }
}
