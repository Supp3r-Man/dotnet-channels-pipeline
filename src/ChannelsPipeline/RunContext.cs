using System.Collections.Concurrent;
using System.Threading.Channels;

namespace ChannelsPipeline;

/// <summary>An item travelling through the pipeline, tagged with its source sequence number.</summary>
internal readonly record struct Envelope<T>(long Seq, T Value);

/// <summary>A stage's bounded input queue. Tracks the deepest depth seen right after an enqueue.</summary>
internal sealed class InputQueue<T>
{
    private readonly Channel<Envelope<T>> _channel;
    private int _maxDepth;
    private int _writerCompleted;

    public InputQueue(int capacity)
    {
        Capacity = capacity;
        _channel = Channel.CreateBounded<Envelope<T>>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
        });
    }

    public int Capacity { get; }
    public ChannelReader<Envelope<T>> Reader => _channel.Reader;
    public int Depth => _channel.Reader.Count;
    public int MaxDepth => Volatile.Read(ref _maxDepth);
    public bool WriterCompleted => Volatile.Read(ref _writerCompleted) == 1;

    public async ValueTask WriteAsync(Envelope<T> item, CancellationToken ct)
    {
        await _channel.Writer.WriteAsync(item, ct).ConfigureAwait(false);
        var depth = _channel.Reader.Count;
        int seen;
        while (depth > (seen = Volatile.Read(ref _maxDepth)))
        {
            if (Interlocked.CompareExchange(ref _maxDepth, depth, seen) == seen) break;
        }
    }

    public void Complete()
    {
        Volatile.Write(ref _writerCompleted, 1);
        _channel.Writer.TryComplete();
    }
}

/// <summary>Shared state of one run: cancellation, accounting, dead letters, first fatal error.</summary>
internal sealed class RunContext : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly ConcurrentDictionary<long, byte> _pending = new();
    private readonly SemaphoreSlim _deadLetterLock = new(1, 1);
    private readonly List<DeadLetter> _deadLetters = new();
    private Exception? _fatal;
    private long _accepted;
    private long _delivered;
    private long _deadLettered;
    private int _aborted;

    public RunContext(PipelineOptions options, CancellationToken external)
    {
        Options = options;
        Time = options.TimeProvider;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(external);
    }

    public PipelineOptions Options { get; }
    public TimeProvider Time { get; }
    public CancellationToken Token => _cts.Token;
    public bool FailFast => Options.FailureMode == FailureMode.FailFast;
    public Exception? Fatal => Volatile.Read(ref _fatal);
    public bool Aborted => Volatile.Read(ref _aborted) == 1;
    public long Accepted => Interlocked.Read(ref _accepted);
    public long Delivered => Interlocked.Read(ref _delivered);
    public long DeadLettered => Interlocked.Read(ref _deadLettered);
    public List<IStageRunner> Stages { get; } = new();
    public Func<Task>? StartSource { get; set; }

    /// <summary>Registers a freshly pulled source item; returns its sequence number.</summary>
    public long Accept()
    {
        var seq = Interlocked.Increment(ref _accepted) - 1;
        _pending[seq] = 0;
        return seq;
    }

    public void MarkDelivered(long seq)
    {
        _pending.TryRemove(seq, out _);
        Interlocked.Increment(ref _delivered);
    }

    public void MarkAborted() => Volatile.Write(ref _aborted, 1);

    /// <summary>Records the first fatal error; optionally cancels the whole run.</summary>
    public void Fail(Exception error, bool cancel)
    {
        Interlocked.CompareExchange(ref _fatal, error, null);
        if (cancel) _cts.Cancel();
    }

    public async ValueTask DeadLetterAsync(DeadLetter deadLetter, CancellationToken ct)
    {
        await _deadLetterLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (Options.DeadLetterSink is { } sink)
            {
                try
                {
                    await sink.WriteAsync(deadLetter, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    Fail(new DeadLetterSinkException(ex), cancel: true);
                    ct.ThrowIfCancellationRequested();
                    throw;
                }
            }

            lock (_deadLetters)
            {
                if (_deadLetters.Count < Options.MaxReportedDeadLetters) _deadLetters.Add(deadLetter);
            }


            _pending.TryRemove(deadLetter.Sequence, out _);
            Interlocked.Increment(ref _deadLettered);
        }
        finally
        {
            _deadLetterLock.Release();
        }
    }

    public IReadOnlyList<DeadLetter> DeadLettersSnapshot()
    {
        lock (_deadLetters) { return _deadLetters.ToArray(); }
    }

    public IReadOnlyList<long> PendingSequences()
    {
        var list = _pending.Keys.ToList();
        list.Sort();
        return list;
    }

    public RunSnapshot Snapshot()
    {
        // Terminal counters first, accepted last: an item is accepted before it can be delivered or dead-lettered, so this
        // read order can never show delivered + dead-lettered > accepted (the reverse order can, because items keep moving).
        var delivered = Delivered;
        var deadLettered = DeadLettered;
        var accepted = Accepted;
        return new RunSnapshot(accepted, delivered, deadLettered, Stages.Select(s => s.Snapshot()).ToArray());
    }

    public void Dispose() => _cts.Dispose();
}
