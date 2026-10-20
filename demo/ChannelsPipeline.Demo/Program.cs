using System.Diagnostics;
using System.Globalization;
using ChannelsPipeline;
using ChannelsPipeline.Demo;

// Usage: dotnet run -c Release --project demo/ChannelsPipeline.Demo -- [--records N] [--write-ms M] [--write-workers W] [--contrast-records N] [--sample-ms S]
var records = Arg("--records", 100_000);
var writeMs = Arg("--write-ms", 5);
var writeWorkers = Arg("--write-workers", 64);
var contrastRecords = Arg("--contrast-records", 20_000);
var sampleMs = Arg("--sample-ms", 2000);
var failureRecords = Arg("--failure-records", 20_000);
var ok = true;

Console.WriteLine($"ChannelsPipeline demo  (.NET {Environment.Version}, {Environment.ProcessorCount} logical cores, synthetic data only)");
Console.WriteLine();

// ------------------------------------------------------------------ part 1: backpressure
Console.WriteLine($"== 1. Backpressure: {records:N0} records, parse -> enrich -> score -> write (write is deliberately slow: {writeMs} ms requested per record, {writeWorkers} workers; Windows timer granularity makes it ~15 ms in practice)");
Console.WriteLine("   queue capacities: parse=64 enrich=64 score=64 write=32 ack(sink)=16");
var bounded = await RunBackpressure(records, writeMs, writeWorkers, capacity: 64, writeCapacity: 32, sampleMs, print: true);
ok &= bounded.Report.IsBalanced && bounded.Report.Delivered == records;
PrintSummary("bounded", bounded);

// ------------------------------------------------------------------ part 2: the same shape without a bound
Console.WriteLine();
Console.WriteLine($"== 2. Contrast: {contrastRecords:N0} records, same stages, queue capacity effectively unbounded ({contrastRecords:N0})");
var unbounded = await RunBackpressure(contrastRecords, writeMs, writeWorkers, capacity: Math.Max(1, contrastRecords), writeCapacity: Math.Max(1, contrastRecords), sampleMs, print: false);
PrintSummary("unbounded", unbounded);
var boundedSmall = await RunBackpressure(contrastRecords, writeMs, writeWorkers, capacity: 64, writeCapacity: 32, sampleMs, print: false);
PrintSummary("bounded (same records)", boundedSmall);
ok &= unbounded.Report.IsBalanced && boundedSmall.Report.IsBalanced;

// ------------------------------------------------------------------ part 3: injected failures
Console.WriteLine();
Console.WriteLine($"== 3. Injected failures: {failureRecords:N0} records, retries (3 attempts, 2 ms backoff) then dead letters");
var failureReport = await RunFailures(failureRecords);
ok &= failureReport.IsBalanced;

// ------------------------------------------------------------------ part 4: fail-fast
Console.WriteLine();
Console.WriteLine("== 4. Fail-fast: the same failures, but the first exhausted item stops the run");
var ff = await RunFailures(failureRecords, FailureMode.FailFast);
ok &= ff.IsBalanced && ff.Outcome == RunOutcome.Faulted;

Console.WriteLine();
Console.WriteLine(ok ? "demo checks passed: every run balanced (delivered + dead-lettered + not-processed = accepted)" : "DEMO CHECK FAILED");
return ok ? 0 : 1;

// ====================================================================================================

int Arg(string name, int fallback)
{
    var args = Environment.GetCommandLineArgs();
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? int.Parse(args[i + 1], CultureInfo.InvariantCulture) : fallback;
}

static async Task<BackpressureResult> RunBackpressure(int count, int writeMs, int writeWorkers, int capacity, int writeCapacity, int sampleMs, bool print)
{
    var peakHeap = 0L;
    var maxResident = 0L;
    var written = 0L;
    var options = new PipelineOptions { SinkOptions = new StageOptions { Capacity = 16 } };
    var stopwatch = Stopwatch.StartNew();
    var startHeap = GC.GetTotalMemory(forceFullCollection: true);

    var run = Pipeline.From(Synthetic.Records(count), options)
        .Stage("parse", (RawRecord r, CancellationToken _) => ValueTask.FromResult(Synthetic.Parse(r)),
            new StageOptions { Capacity = capacity, Parallelism = 4 })
        .Stage("enrich", (Parsed p, CancellationToken _) => ValueTask.FromResult(Synthetic.Enrich(p)),
            new StageOptions { Capacity = capacity, Parallelism = 4 })
        .Stage("score", (Enriched e, CancellationToken _) => ValueTask.FromResult(Synthetic.Score(e)),
            new StageOptions { Capacity = capacity, Parallelism = 4 })
        .Stage("write", async (Scored s, CancellationToken ct) =>
        {
            if (writeMs > 0) await Task.Delay(writeMs, ct);   // pretend this is a slow database or API
            return s.Id;
        }, new StageOptions { Capacity = writeCapacity, Parallelism = writeWorkers, Ordered = false })
        .Start((id, _) =>
        {
            Interlocked.Increment(ref written);
            return ValueTask.CompletedTask;
        });

    var sampler = Task.Run(async () =>
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(sampleMs));
        while (await timer.WaitForNextTickAsync() && !run.Completion.IsCompleted)
        {
            var snap = run.Snapshot();
            var heap = GC.GetTotalMemory(false);
            peakHeap = Math.Max(peakHeap, heap);
            var resident = snap.Accepted - snap.Delivered - snap.DeadLettered;
            maxResident = Math.Max(maxResident, resident);
            if (print)
            {
                var q = string.Join("/", snap.Stages.Select(s => s.QueueDepth.ToString(CultureInfo.InvariantCulture)));
                Console.WriteLine($"   t={stopwatch.Elapsed.TotalSeconds,5:F1}s  accepted={snap.Accepted,8:N0}  delivered={snap.Delivered,8:N0}  in-flight={resident,4}  queue depth (parse/enrich/score/write/ack)={q,-14}  managed heap={heap / 1048576.0,5:F1} MB");
            }
        }
    });

    var report = await run.Completion;
    await sampler;
    stopwatch.Stop();
    peakHeap = Math.Max(peakHeap, GC.GetTotalMemory(false));
    return new BackpressureResult(report, stopwatch.Elapsed, peakHeap - startHeap, maxResident, written);
}

static void PrintSummary(string label, BackpressureResult r)
{
    var rep = r.Report;
    Console.WriteLine($"   [{label}] {rep.Outcome}: accepted={rep.Accepted:N0} delivered={rep.Delivered:N0} dead-lettered={rep.DeadLettered} not-processed={rep.NotProcessed} in {r.Elapsed.TotalSeconds:F1}s  ({rep.Delivered / r.Elapsed.TotalSeconds:N0} records/s on this machine, one run, not a benchmark)");
    Console.WriteLine($"   [{label}] max queue depth observed / capacity: " +
        string.Join("  ", rep.Stages.Select(s => $"{s.Name}={s.MaxQueueDepth}/{s.QueueCapacity}")));
    Console.WriteLine($"   [{label}] peak in-flight items sampled: {r.MaxResident:N0}; peak managed heap above start: {r.PeakHeapDelta / 1048576.0:F1} MB (includes uncollected garbage, so indicative only)");
}

static async Task<RunReport> RunFailures(int count, FailureMode mode = FailureMode.Continue)
{
    var external = new InMemoryDeadLetterSink();
    var attempts = new System.Collections.Concurrent.ConcurrentDictionary<(string, long), int>();
    var retry = RetryPolicy.Fixed(3, TimeSpan.FromMilliseconds(2));
    var options = new PipelineOptions { FailureMode = mode, DeadLetterSink = external, MaxReportedDeadLetters = 5, SinkOptions = new StageOptions { Retry = retry } };
    var delivered = 0L;

    var report = await Pipeline.From(Synthetic.Records(count), options)
        .Stage("parse", (RawRecord r, CancellationToken _) =>
        {
            // ~1% fail on the first attempt only (transient): retried and succeed.
            if (Synthetic.Chance("parse", r.Id) < 0.01 && attempts.AddOrUpdate(("parse", r.Id), 1, (_, n) => n + 1) == 1)
                throw new IOException($"transient read error on record {r.Id}");
            return ValueTask.FromResult(Synthetic.Parse(r));
        }, new StageOptions { Capacity = 64, Parallelism = 4, Retry = retry })
        .Stage("enrich", (Parsed p, CancellationToken _) =>
        {
            // ~0.3% are permanently bad: exhaust all 3 attempts and are dead-lettered.
            if (Synthetic.Chance("enrich", p.Id) < 0.003) throw new InvalidDataException($"unknown customer for record {p.Id}");
            return ValueTask.FromResult(Synthetic.Enrich(p));
        }, new StageOptions { Capacity = 64, Parallelism = 4, Retry = retry })
        .Stage("score", (Enriched e, CancellationToken _) => ValueTask.FromResult(Synthetic.Score(e)),
            new StageOptions { Capacity = 64, Parallelism = 4 })
        .RunAsync((s, _) =>
        {
            // ~0.5% fail twice then succeed (retried by the sink stage).
            if (Synthetic.Chance("write", s.Id) < 0.005 && attempts.AddOrUpdate(("write", s.Id), 1, (_, n) => n + 1) <= 2)
                throw new TimeoutException($"write timeout for record {s.Id}");
            Interlocked.Increment(ref delivered);
            return ValueTask.CompletedTask;
        });
    PrintFailureReport("main run", report, external.Items.Count);
    return report;
}

static void PrintFailureReport(string label, RunReport r, int externalCount)
{
    Console.WriteLine($"   [{label}] {r.Outcome}: accepted={r.Accepted:N0} delivered={r.Delivered:N0} dead-lettered={r.DeadLettered} not-processed={r.NotProcessed} balanced={r.IsBalanced}  elapsed={r.Elapsed.TotalSeconds:F1}s");
    if (r.FatalError is not null) Console.WriteLine($"   [{label}] fatal error: {r.FatalError.GetType().Name}: {r.FatalError.Message}");
    foreach (var s in r.Stages)
        Console.WriteLine($"   [{label}]   {s.Name,-7} in={s.In,7:N0} out={s.Out,7:N0} failed={s.Failed,4} retried={s.Retried,4} maxQueue={s.MaxQueueDepth}/{s.QueueCapacity} busyTime={s.BusyTime.TotalMilliseconds:F0}ms");
    Console.WriteLine($"   [{label}] dead-letter sink received {externalCount} item(s); first {r.DeadLetters.Count}:");
    foreach (var d in r.DeadLetters)
        Console.WriteLine($"   [{label}]     #{d.Sequence} stage={d.Stage} attempts={d.Attempts} {d.Exception.GetType().Name}: {d.Exception.Message}");
}

internal sealed record BackpressureResult(RunReport Report, TimeSpan Elapsed, long PeakHeapDelta, long MaxResident, long Written);
