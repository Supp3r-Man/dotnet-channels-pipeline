using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace ChannelsPipeline;

/// <summary>Fan-in / fan-out helpers that operate on async sequences, so they compose with <see cref="Pipeline.From{T}(IAsyncEnumerable{T}, PipelineOptions?)"/>.</summary>
public static class Fan
{
    /// <summary>
    /// Groups items into lists. A batch is emitted when it reaches <paramref name="size"/> items, or when
    /// <paramref name="maxWait"/> has elapsed (on <paramref name="timeProvider"/>) since its first item arrived, or when
    /// the source ends (the remainder). Empty batches are never emitted. Pass <see cref="Timeout.InfiniteTimeSpan"/> to flush by size only.
    /// </summary>
    public static IAsyncEnumerable<IReadOnlyList<T>> Batch<T>(this IAsyncEnumerable<T> source, int size, TimeSpan maxWait, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentOutOfRangeException.ThrowIfLessThan(size, 1);
        if (maxWait != Timeout.InfiniteTimeSpan) ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxWait, TimeSpan.Zero);
        return BatchCore(source, size, maxWait, timeProvider ?? TimeProvider.System);
    }

    private static async IAsyncEnumerable<IReadOnlyList<T>> BatchCore<T>(IAsyncEnumerable<T> source, int size, TimeSpan maxWait,
        TimeProvider time, [EnumeratorCancellation] CancellationToken ct = default)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var enumerator = source.GetAsyncEnumerator(linked.Token);
        Task<bool>? pending = null;        // an outstanding MoveNextAsync; it survives a time flush
        Task? timer = null;                // armed when the first item of a batch arrives
        CancellationTokenSource? timerCts = null;
        var batch = new List<T>(Math.Min(size, 1024));
        var timed = maxWait != Timeout.InfiniteTimeSpan;
        try
        {
            while (true)
            {
                pending ??= enumerator.MoveNextAsync().AsTask();
                if (timer is not null && !pending.IsCompleted)
                {
                    await Task.WhenAny(pending, timer).ConfigureAwait(false);
                    if (!pending.IsCompleted && timer.IsCompleted)
                    {
                        // Time flush. The batch is non-empty because the timer only exists while it holds items.
                        DisarmTimer(ref timer, ref timerCts);
                        yield return Take(batch);
                        continue;
                    }
                }

                if (!await pending.ConfigureAwait(false)) break;
                pending = null;
                batch.Add(enumerator.Current);
                if (batch.Count >= size)
                {
                    DisarmTimer(ref timer, ref timerCts);
                    yield return Take(batch);
                }
                else if (timed && timer is null)
                {
                    timerCts = CancellationTokenSource.CreateLinkedTokenSource(linked.Token);
                    timer = Task.Delay(maxWait, time, timerCts.Token);
                }
            }

            pending = null;
            DisarmTimer(ref timer, ref timerCts);
            if (batch.Count > 0) yield return Take(batch);
        }
        finally
        {
            linked.Cancel();
            DisarmTimer(ref timer, ref timerCts);
            if (pending is not null)
            {
                try { await pending.ConfigureAwait(false); } catch { /* the consumer left; the source's error no longer matters */ }
            }

            await enumerator.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static IReadOnlyList<T> Take<T>(List<T> batch)
    {
        var copy = batch.ToArray();
        batch.Clear();
        return copy;
    }

    private static void DisarmTimer(ref Task? timer, ref CancellationTokenSource? timerCts)
    {
        timerCts?.Cancel();
        timerCts?.Dispose();
        timerCts = null;
        timer = null;
    }

    /// <summary>Merges sources into one sequence in arrival order (each source's own order is preserved).</summary>
    public static IAsyncEnumerable<T> Merge<T>(params IAsyncEnumerable<T>[] sources) => Merge(sources, 16);

    /// <summary>
    /// Merges sources into one sequence. Each source is pumped by its own task into a shared bounded queue of
    /// <paramref name="capacity"/> items, so a slow consumer slows every source. If any source throws, the others are
    /// cancelled and the consumer sees that exception after already-queued items. Disposing the merged enumerator cancels and awaits all pumps.
    /// </summary>
    public static IAsyncEnumerable<T> Merge<T>(IReadOnlyList<IAsyncEnumerable<T>> sources, int capacity)
    {
        ArgumentNullException.ThrowIfNull(sources);
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        foreach (var s in sources) ArgumentNullException.ThrowIfNull(s);
        return MergeCore(sources.ToArray(), capacity);
    }

    private static async IAsyncEnumerable<T> MergeCore<T>(IAsyncEnumerable<T>[] sources, int capacity, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity) { SingleReader = true });
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Exception? firstError = null;

        var pumps = sources.Select(source => Task.Run(async () =>
        {
            try
            {
                await foreach (var item in source.WithCancellation(cts.Token).ConfigureAwait(false))
                    await channel.Writer.WriteAsync(item, cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            {
                // cancelled by the consumer or by a sibling's failure
            }
            catch (Exception ex)
            {
                Interlocked.CompareExchange(ref firstError, ex, null);
                cts.Cancel();
            }
        })).ToArray();

        var closer = Task.Run(async () =>
        {
            await Task.WhenAll(pumps).ConfigureAwait(false);
            channel.Writer.TryComplete(Volatile.Read(ref firstError));
        });

        try
        {
            await foreach (var item in channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                yield return item;
        }
        finally
        {
            cts.Cancel();
            await closer.ConfigureAwait(false);
        }
    }
}
