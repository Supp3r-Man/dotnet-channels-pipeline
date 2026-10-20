# dotnet-channels-pipeline

A small, honest library for **staged, backpressured data processing on `System.Threading.Channels`** (C# / .NET 10), with a console demo and an xunit suite.

You describe a pipeline as a chain of stages. Every stage has a **bounded input queue**, a configurable number of **parallel workers**, **ordered or unordered** output, **per-item retry** with a policy, a **dead-letter sink**, and an optional **per-item timeout**. The whole run is cooperatively cancellable and ends in a **`RunReport`** that accounts for every item the pipeline accepted: delivered, dead-lettered, or not processed (and exactly which).

It is an in-process teaching/portfolio project, not a replacement for TPL Dataflow, Akka Streams, or a message broker. See [Known limits](#known-limits).

```
source ──pump──▶ [queue c1]──▶ stage 1 (p1 workers) ──▶ [queue c2]──▶ stage 2 (p2 workers) ──▶ … ──▶ [queue c_sink]──▶ sink
         blocks when a queue is full, so a slow last stage slows the source (backpressure)
```

## Usage

```csharp
using ChannelsPipeline;

var deadLetters = new InMemoryDeadLetterSink();
var options = new PipelineOptions
{
    FailureMode = FailureMode.Continue,          // or FailFast
    DeadLetterSink = deadLetters,                // optional; the report also keeps the first N
    TimeProvider = TimeProvider.System,          // inject FakeTimeProvider in tests
};

RunReport report = await Pipeline
    .From(ReadRecordsAsync(), options)           // any IAsyncEnumerable<T> (or IEnumerable<T>)
    .Stage("parse",  (Raw r, CancellationToken ct) => ValueTask.FromResult(Parse(r)),
           new StageOptions { Capacity = 64, Parallelism = 4 })                 // ordered by default
    .Stage("enrich", async (Parsed p, CancellationToken ct) => await LookupAsync(p, ct),
           new StageOptions
           {
               Capacity = 64, Parallelism = 8, Ordered = false,                 // unordered: no reorder buffer
               Retry = RetryPolicy.Exponential(maxAttempts: 4, baseDelay: TimeSpan.FromMilliseconds(50)),
               ItemTimeout = TimeSpan.FromSeconds(2),                           // cooperative, via the token
           })
    .RunAsync(async (Enriched e, CancellationToken ct) => await WriteAsync(e, ct), cancellationToken);

Console.WriteLine($"{report.Outcome}: delivered={report.Delivered} dead={report.DeadLettered} not-processed={report.NotProcessed}");
foreach (var d in report.DeadLetters) Console.WriteLine($"{d.Stage} #{d.Sequence} x{d.Attempts}: {d.Exception.Message}");
report.ThrowIfNotCompleted();                    // optional: turn Cancelled/Faulted into exceptions
```

* `Pipeline.From(source, options)` returns an immutable, reusable `PipelineBuilder<T>`; `.Stage(name, fn, options)` returns a builder for the new output type.
* `.RunAsync(sink, ct)` runs to the end. `.Start(sink, ct)` returns a `PipelineRun` with `Snapshot()` (live per-stage metrics) and `Completion` (the report).
* The **sink** is the last stage (named `"sink"`): it has its own `StageOptions` (`PipelineOptions.SinkOptions`: queue capacity, parallelism, retry, timeout), and its exceptions are retried and dead-lettered like any other stage's.
* `Fan.Batch(source, size, maxWait, timeProvider)` and `Fan.Merge(sources…)` operate on `IAsyncEnumerable<T>` and compose with `Pipeline.From`.
* `RunAsync` **never throws for item failures, cancellation, or a faulted run**: the report is the result. Look at `report.Outcome` (`Completed`, `Cancelled`, `Faulted`) and `report.FatalError`.

## Design notes

### Backpressure math

Every stage owns one `Channel.CreateBounded` input queue (`FullMode = Wait`). Whoever feeds it (the source pump, or the previous stage's workers) awaits `WriteAsync`, so a full queue suspends the producer; nothing is buffered without bound and nothing is dropped.

The number of items that can be inside the pipeline at once is therefore bounded by

```
in-flight ≤ 1 (the item the source pump holds while blocked)
          + Σ over stages (queue capacity + workers)          // unordered, or ordered with one worker
          + Σ over ordered multi-worker stages (ReorderWindow − workers)   // the window replaces the worker count
          + sink (capacity + workers)
```

For a single gated stage this is exactly `capacity + parallelism + 1`: the tests make the stage block, wait until the source has been pulled that many times, assert it is *exactly* that number, then release one item at a time and assert exactly one more is pulled each time (`BackpressureTests`). For the demo shape below (three ordered stages with 4 workers and the default window of 16, capacity 64, then `write` with capacity 32 and 64 workers, and a sink with capacity 16 and 1 worker) the bound is `1 + 3×(64+16) + (32+64) + (16+1) = 354` items; the sampler saw at most 337 in flight.

### Ordered output and its buffer bound

`Ordered = true` (the default) with `Parallelism > 1` must put results back in the order items entered the stage, without letting a slow item make a buffer grow forever. The mechanism:

* A worker takes an item from the input queue and gets an **ordinal** under a lock (dequeue and ordinal assignment are atomic, so ordinals follow the queue's FIFO order).
* Before taking an item a worker acquires a permit from a `SemaphoreSlim(ReorderWindow)`. A permit is released only after that item's result has been **emitted downstream** (or skipped because it was dead-lettered). So the stage never holds more than `ReorderWindow` items: in progress + finished-and-waiting + being emitted.
* Finished results wait in a dictionary keyed by ordinal. Whoever completes the next-expected ordinal becomes the single "drainer" and emits every consecutive result; dead-lettered items leave a tombstone so they cannot stall the sequence.

**Bound:** the reorder buffer never holds more than `ReorderWindow` results (default `4 × Parallelism`, minimum `Parallelism`), and at most `ReorderWindow − 1` while the oldest unfinished item is still running. Tests assert both: a gated head item makes the buffer fill to *exactly* `ReorderWindow − 1` and the stage then refuses to start more items than the window allows; random-delay runs assert `MaxReorderBuffered ≤ ReorderWindow` for every window/parallelism combination tried.

**Cost:** head-of-line blocking. If one item takes very long, after `ReorderWindow` items the stage stalls until it finishes. A bigger window trades memory for smoothness; `Ordered = false` has no such cost and no buffer. "Ordered" means *the order in which items arrived at that stage*; after an unordered stage, arrival order is whatever that stage emitted. The sink ignores `Ordered` (nothing is downstream of it); with `Parallelism > 1` sink calls run concurrently.

### Retry and dead-letter semantics

* `RetryPolicy(MaxAttempts, Backoff(failedAttempt), ShouldRetry(exception))`. `MaxAttempts` counts the first try, so `1` means no retry. Helpers: `None`, `Fixed`, `Exponential` (doubling, optional cap).
* Retries happen **inside the worker that owns the item** (it is not re-queued). That keeps ordering and backpressure intact and keeps the attempt count exact; the price is that the worker is occupied during backoff. Backoff waits use `TimeProvider`, so tests drive them with a fake clock.
* After the last failed attempt the item is **dead-lettered**: `DeadLetter(Sequence, Stage, Item, Attempts, Exception)`, where `Item` is the input that stage received, `Attempts` is the exact number of invocations, and `Exception` is the last attempt's exception instance. Dead letters go to the optional `IDeadLetterSink` (writes are serialized, so it need not be thread-safe) and to the report (first `MaxReportedDeadLetters`, the count is always exact).
* If the dead-letter sink itself throws, that is fatal (`DeadLetterSinkException`): the item could not be accounted for, so it is *not* counted as dead-lettered and shows up as not processed.
* `ItemTimeout` is **cooperative**: a linked token is cancelled after the timeout (on `TimeProvider`) and the attempt fails with `ItemTimeoutException` (a `TimeoutException`). A function that ignores its token is not interrupted. Outer cancellation is never mistaken for a timeout.
* `FailFast`: the first item that exhausts its retries is dead-lettered and then cancels the whole run; `report.FatalError` is a `StageFailedException` (stage, sequence, attempts, inner exception). `Continue` keeps going.

### Shutdown guarantees

* **Graceful drain.** When the source is exhausted the pump completes the first queue; each stage completes its output queue when all its workers are done, so items flow all the way to the sink and every queue is completed. A completed run satisfies `delivered + dead-lettered = accepted`.
* **Every accepted item is accounted for.** `RunReport` has `Accepted`, `Delivered`, `DeadLettered`, and `NotProcessedSequences` (the exact source sequence numbers). `IsBalanced` is `Accepted == Delivered + DeadLettered + NotProcessed`. This holds after cancellation and fatal errors too, and a seeded random property test checks it over hundreds of random pipeline shapes, failure rates and cancel points.
* **Cancellation is cooperative and returns a report.** Cancelling the token stops the pump, wakes every blocked worker (queue waits, retry backoff, reorder-window waits all observe the token), and `RunAsync` returns only after every worker task and the source enumerator have finished. Outcome `Cancelled`.
* **A throwing source** stops intake. Under `Continue` the items already accepted are drained (outcome `Faulted`, `SourceFailedException`); under `FailFast` the run is cancelled.
* **"Not processed" means "not confirmed delivered".** An item whose sink call was interrupted may or may not have had its side effect. The sink is counted delivered only when its function returns, with no await in between, so cancellation cannot split "returned" from "counted".
* **Batching.** `Fan.Batch` flushes by size, by `maxWait` since the first item of the batch (on `TimeProvider`), and at the end of the source. If the source throws, the partial batch is dropped and the exception propagates. Batches are the pipeline's items, so "accepted" counts batches.

## Demo

`demo/ChannelsPipeline.Demo` pushes 100,000 synthetic records (each carries a 1 KB payload) through `parse → enrich → score → write` with a deliberately slow `write` stage, printing queue depth every 2 s; then contrasts with effectively unbounded queues; then injects failures; then shows fail-fast.

```
dotnet run -c Release --project demo/ChannelsPipeline.Demo
dotnet run -c Release --project demo/ChannelsPipeline.Demo -- --records 3000 --write-ms 1 --contrast-records 2000 --failure-records 3000   # quick
```

Real output of one run on one laptop (Windows 11, 8 logical cores, .NET 10, Release). **These throughput numbers are not a benchmark**: the `write` stage is a timer delay, so records/s mostly reflects Windows timer granularity (a 5 ms `Task.Delay` takes ~15 ms) times 64 workers, not what the library can do.

```
ChannelsPipeline demo  (.NET 10.0.13, 8 logical cores, synthetic data only)

== 1. Backpressure: 100,000 records, parse -> enrich -> score -> write (write is deliberately slow: 5 ms requested per record, 64 workers; Windows timer granularity makes it ~15 ms in practice)
   queue capacities: parse=64 enrich=64 score=64 write=32 ack(sink)=16
   t=  2.1s  accepted=   8,337  delivered=   8,000  in-flight= 337  queue depth (parse/enrich/score/write/ack)=64/64/64/32/0   managed heap=  6.3 MB
   t=  4.0s  accepted=  16,247  delivered=  15,974  in-flight= 273  queue depth (parse/enrich/score/write/ack)=46/63/48/32/0   managed heap=  3.4 MB
   t=  6.0s  accepted=  24,327  delivered=  23,998  in-flight= 329  queue depth (parse/enrich/score/write/ack)=64/64/58/32/16  managed heap=  0.6 MB
   t=  8.0s  accepted=  32,304  delivered=  31,974  in-flight= 330  queue depth (parse/enrich/score/write/ack)=64/64/52/22/16  managed heap=  3.3 MB
   t= 10.0s  accepted=  40,368  delivered=  40,046  in-flight= 322  queue depth (parse/enrich/score/write/ack)=64/64/41/31/16  managed heap=  6.3 MB
   t= 12.0s  accepted=  48,432  delivered=  48,095  in-flight= 337  queue depth (parse/enrich/score/write/ack)=64/64/64/17/16  managed heap=  3.4 MB
   t= 14.0s  accepted=  56,560  delivered=  56,232  in-flight= 328  queue depth (parse/enrich/score/write/ack)=64/64/64/31/16  managed heap=  6.5 MB
   t= 16.0s  accepted=  64,610  delivered=  64,286  in-flight= 324  queue depth (parse/enrich/score/write/ack)=64/64/48/24/14  managed heap=  3.5 MB
   t= 18.0s  accepted=  72,610  delivered=  72,273  in-flight= 337  queue depth (parse/enrich/score/write/ack)=64/64/64/17/16  managed heap=  0.8 MB
   t= 20.0s  accepted=  80,738  delivered=  80,401  in-flight= 337  queue depth (parse/enrich/score/write/ack)=64/64/64/17/16  managed heap=  4.3 MB
   t= 22.0s  accepted=  88,802  delivered=  88,465  in-flight= 337  queue depth (parse/enrich/score/write/ack)=64/64/64/32/0   managed heap=  1.6 MB
   t= 24.0s  accepted=  96,866  delivered=  96,529  in-flight= 337  queue depth (parse/enrich/score/write/ack)=64/64/64/27/6   managed heap=  4.7 MB
   [bounded] Completed: accepted=100,000 delivered=100,000 dead-lettered=0 not-processed=0 in 26.0s  (3,839 records/s on this machine, one run, not a benchmark)
   [bounded] max queue depth observed / capacity: parse=64/64  enrich=64/64  score=64/64  write=32/32  sink=16/16
   [bounded] peak in-flight items sampled: 337; peak managed heap above start: 6.4 MB (includes uncollected garbage, so indicative only)

== 2. Contrast: 20,000 records, same stages, queue capacity effectively unbounded (20,000)
   [unbounded] Completed: accepted=20,000 delivered=20,000 dead-lettered=0 not-processed=0 in 6.0s  (3,327 records/s on this machine, one run, not a benchmark)
   [unbounded] max queue depth observed / capacity: parse=703/20000  enrich=220/20000  score=837/20000  write=19616/20000  sink=16/16
   [unbounded] peak in-flight items sampled: 12,062; peak managed heap above start: 26.3 MB (includes uncollected garbage, so indicative only)
   [bounded (same records)] Completed: accepted=20,000 delivered=20,000 dead-lettered=0 not-processed=0 in 6.0s  (3,329 records/s on this machine, one run, not a benchmark)
   [bounded (same records)] max queue depth observed / capacity: parse=64/64  enrich=64/64  score=64/64  write=32/32  sink=16/16
   [bounded (same records)] peak in-flight items sampled: 337; peak managed heap above start: 6.8 MB (includes uncollected garbage, so indicative only)

== 3. Injected failures: 20,000 records, retries (3 attempts, 2 ms backoff) then dead letters
   [main run] Completed: accepted=20,000 delivered=19,934 dead-lettered=66 not-processed=0 balanced=True  elapsed=5.6s
   [main run]   parse   in= 20,000 out= 20,000 failed=   0 retried= 207 maxQueue=64/64 busyTime=3235ms
   [main run]   enrich  in= 20,000 out= 19,934 failed=  66 retried= 132 maxQueue=64/64 busyTime=2089ms
   [main run]   score   in= 19,934 out= 19,934 failed=   0 retried=   0 maxQueue=64/64 busyTime=6ms
   [main run]   sink    in= 19,934 out= 19,934 failed=   0 retried= 214 maxQueue=16/16 busyTime=3336ms
   [main run] dead-letter sink received 66 item(s); first 5:
   [main run]     #180 stage=enrich attempts=3 InvalidDataException: unknown customer for record 180
   [main run]     #237 stage=enrich attempts=3 InvalidDataException: unknown customer for record 237
   [main run]     #264 stage=enrich attempts=3 InvalidDataException: unknown customer for record 264
   [main run]     #496 stage=enrich attempts=3 InvalidDataException: unknown customer for record 496
   [main run]     #937 stage=enrich attempts=3 InvalidDataException: unknown customer for record 937

== 4. Fail-fast: the same failures, but the first exhausted item stops the run
   [main run] Faulted: accepted=341 delivered=180 dead-lettered=1 not-processed=160 balanced=True  elapsed=0.1s
   [main run] fatal error: StageFailedException: Stage 'enrich' failed item #180 after 3 attempt(s): unknown customer for record 180
   [main run]   parse   in=    276 out=    260 failed=   0 retried=   4 maxQueue=64/64 busyTime=53ms
   [main run]   enrich  in=    196 out=    180 failed=   1 retried=   2 maxQueue=64/64 busyTime=33ms
   [main run]   score   in=    180 out=    180 failed=   0 retried=   0 maxQueue=64/64 busyTime=0ms
   [main run]   sink    in=    180 out=    180 failed=   0 retried=   0 maxQueue=16/16 busyTime=0ms
   [main run] dead-letter sink received 1 item(s); first 1:
   [main run]     #180 stage=enrich attempts=3 InvalidDataException: unknown customer for record 180

demo checks passed: every run balanced (delivered + dead-lettered + not-processed = accepted)
```

What to read from it: with bounded queues the three upstream queues sit at their 64 capacity and the in-flight count stays around 330 for the whole 100k records while the slow stage sets the pace; the unbounded contrast let 19,616 records pile up in front of `write` and the managed heap peak was about four times higher (indicative only: `GC.GetTotalMemory(false)` includes uncollected garbage). Total elapsed time is about the same, because the slow stage is the bottleneck either way: backpressure bounds memory, it does not make the bottleneck faster.

## How it is tested

`dotnet test`: **238 test cases** (153 test methods, 20 of them theories) in xunit v3 on the Microsoft Testing Platform runner. No `Thread.Sleep`, no arbitrary `Task.Delay`: tests synchronize with `TaskCompletionSource` gates, `SemaphoreSlim` turnstiles, `Barrier`, awaitable counters, and a `FakeTimeProvider` wrapper that tells the test when a timer has actually been created, so advancing the clock is never racy. Every test has a 60 s `Timeout`, so a hang fails instead of blocking.

| Area | Cases | What is proved |
|---|---|---|
| Options / API / basics | 55 | validation, defaults, reorder-window defaults, retry-policy math, duplicate/reserved stage names, empty source, type-changing chains |
| Backpressure | 11 | a gated stage lets the source get exactly `capacity + workers + 1` ahead, each release admits exactly one more; the multi-stage and ordered-window bounds; max queue depth ≤ capacity; cancellation releases a blocked producer |
| Ordering | 29 | ordered output under seeded random delays; buffer ≤ window; gated head fills the buffer to exactly `window − 1`; dead-lettered head does not stall; unordered delivers each item exactly once and does not wait for a slow head |
| Retry / dead letter / timeout | 24 | exact attempt counts (1 to 6), succeed-on-Nth, `ShouldRetry`, backoff driven by a fake clock (fixed and exponential), dead-letter record contents, external sink, cap, serialized writes, throwing sink, item timeouts |
| Failure modes / exception types | 29 | continue vs fail-fast, throwing source (both modes), throwing sink, sink retry, nine exception types preserved by instance, `StageFailedException` fields, throwing retry callback |
| Cancellation + random property test | 20 | mid-flight cancel, before start, after completion, during backoff, blocked sink, ordered head gated; 10 theory blocks × 30 seeds of random pipelines (0 to 4 stages, random capacity/parallelism/order/retry/failure rate/failure mode/cancel point) checked against a deterministic failure oracle |
| Batch / Merge | 35 | size/time/remainder flush with a fake clock, no empty batches, source error, early exit disposes source, merge exactly-once and per-source order, failing sibling cancelled, merge backpressure |
| Metrics / leaks | 12 |
| Added after the first mutation run (argument checks, messages, backoff clamping, timer disposal, frozen stage time, first-fatal-wins, ...) | 23 | `In = Out + Failed` per stage, busy workers, fake-clock timings, live snapshots; source disposed, every queue completed, all tasks finished in normal / cancelled / fail-fast / source-error runs, 200 short runs |

Run repeatedly on the same machine: the 215-test version passed 12 times in a row and the final 238-test version 6 times in a row (about 3 to 5 s per run). That is evidence against flakiness, not proof; the full-suite mutant re-check did expose one latent race (the live-snapshot counter order) that those repeated runs had not.

## Mutation check

Two tools were used. Nothing here is a proof of correctness: a mutation score says how much of the library's behaviour the tests pin down.

**1. Stryker.NET 5.0.0** (works on .NET 10 with `--test-runner mtp`, installed with `dotnet tool install dotnet-stryker --tool-path .tools/stryker`, run from `tests/ChannelsPipeline.Tests`, 3 parallel runners, about 13 minutes per run).

| Run | Mutants created | Compile error | Ignored | No coverage | Tested | Killed | Timeout (counts as killed) | Survived | Score |
|---|---|---|---|---|---|---|---|---|---|
| 1, first 215 tests | 453 | 77 | 87 | 7 | 282 | 163 | 35 | 83 | 68.75 % |
| 2, after adding 21 gap tests (236 tests) | 453 | 77 | 87 | 7 | 281 | 203 | 25 | 53 | 79.17 % |

What the numbers hide, honestly:

* **Stryker could not mutate the retry loop.** It prints "Safe Mode! Stryker will remove all mutations in ProcessAsync and mark them as compile error", so the 77 compile-error mutants include every mutant of the most interesting method. That region (and the reorder/emit logic) is covered by the hand-rolled script below.
* **Its coverage-based test selection under the MTP runner is unreliable for async iterators.** Of run 1's 83 survivors, 2 were killed when the whole suite was run against them (`scripts/recheck_stryker_survivors.py` re-applies each survivor and runs everything). One of those, `ConfigureAwait(false)` to `true` on a queue write, exposed a **real race in my live snapshot** (it read the accepted counter before the delivered counter, so a snapshot could show delivered above accepted); fixed by reading the terminal counters first. Run 2's 53 survivors: 0 killed by the full suite, 52 truly survived, 1 did not compile when re-applied.
* **Run 2 survivors (53), all examined:** 31 are `ConfigureAwait(false)` to `true` (equivalent: no synchronization context in the library or the tests). 21 are equivalent for a stated reason (one of them did not compile when re-applied): capacity hints and flags that do not change behaviour (`Math.Min` to `Max` for a list's initial capacity, `SingleReader`, the default `FullMode`), a `>` versus `>=` where both branches yield the same value (`delay > cap`, max-depth updates), a `continue` that falls through to identical code, a timer disarmed in two places so each removal alone is redundant (`Fan.cs` lines 65 and 71), a `&&` versus `||` that only differs when an item and a timer complete at the same instant (both orders valid), redundant defence in depth (`MarkAborted` called by both pump and workers; the dead-letter-sink failure path cancelling twice; `ThrowIfCancellationRequested` in the synchronous-source iterator; a permit release on worker exit that no live worker needs), and `ctx.Dispose()` (CTS disposal, not observable without reflection). **1 is a real test gap that I did not close:** `RunContext.PendingSequences` without `list.Sort()` still passes, even with a test written for it (the pending keys happen to enumerate in ascending order from the hash table), so the "ascending" promise of `NotProcessedSequences` is only guaranteed by the code, not pinned by a test.
* The gap tests added after run 1 (null-argument checks, exception messages, exponential-backoff clamping, window overflow, user-cancel versus timeout, frozen stage time, reorder-metric exactness, timer disposal) took the score from 68.75 % to 79.17 %.

**2. Hand-rolled mutants, `scripts/mutation_check.py`** (modelled on the Java concurrency lab's script): 41 single textual replacements, each required to match exactly once, `dotnet test --no-restore` per mutant with a 60 s timeout, restored in a `finally` block; `src/` is backed up before the run and compared byte-for-byte afterwards (it was identical after every run). A mutant that deadlocks or hangs is only killed by the timeout, and those are labelled `killed (timeout)`: the process tree rooted at the `dotnet test` this script started is killed with `taskkill /T`, never by image name. Several "timeout" kills are tests that wait for an exact bound (for example "the source is pulled exactly N times"), which wait forever when the bound changes; they are not deadlocks in the library.

The table merges two runs: the first full run (40 mutants, 8 of which did not compile because removing a statement left an unused field under warnings-as-errors), and a re-run of the 8 repaired mutants plus 3 re-checks after new tests (`#5`, `#28`) and the new sort mutant `#32`. Results: **37 of 41 killed (16 of those kills by timeout), 4 survived**. The 3 survivors besides the sort gap (`#11`, `#12`, `#13`) are the cancellation checks in the retry loop: they are redundant with each other (the check after a failure, the check before an attempt, and the `OperationCanceledException` filter all lead to the same abandonment), so removing any one alone changes nothing observable; I did not write a test that could distinguish them.

| # | Mutation | Result | Killed by (first test) |
|---|---|---|---|
| 1 | retry: one extra attempt (`<` -> `<=`) | killed (timeout) | hang until the per-mutant timeout |
| 2 | retry: one attempt too few | killed (timeout) | hang until the per-mutant timeout |
| 3 | retry: ShouldRetry predicate ignored | killed | MessageTests.FatalErrorMessages_CarryTheirContext |
| 4 | retry: retried-counter not incremented | killed | FailureModeTests.Continue_WhenEveryItemFails_StillCompletes |
| 5 | retry: backoff always uses the first attempt's delay | killed | MutationFollowUpTests.ExponentialBackoff_ArmsItsTimersWithDoublingDueTimes |
| 6 | retry: backoff wait skipped | killed (timeout) | hang until the per-mutant timeout |
| 7 | dead letter: attempt count off by one | killed | PropertyTests.RandomPipelines_AlwaysAccountForEveryAcceptedItem |
| 8 | dead letter: failed-counter not incremented | killed | FailureModeTests.Continue_WhenEveryItemFails_StillCompletes |
| 9 | fail-fast: exhausted item no longer fails the run | killed (timeout) | hang until the per-mutant timeout |
| 10 | fail-fast: StageFailedException reports attempt 1 | killed | FailureModeTests.FailFast_StopsAtTheFirstExhaustedItem_AndReportsEverythingElseAsNotProcessed |
| 11 | retry loop: no cancellation check before each attempt | SURVIVED |  |
| 12 | retry loop: no 'abandon item if cancelled' check after a failure | SURVIVED |  |
| 13 | retry loop: run cancellation treated as an item failure | SURVIVED |  |
| 14 | timeout: wrong exception type (rethrows OperationCanceledException) | killed | RetryTests.ItemTimeout_FailsTheAttemptWithItemTimeoutException |
| 15 | metrics: busy-workers counter never decremented | killed | CancellationTests.CancelMidFlight_LeavesNoStuckTasks_AndAccountsForEveryAcceptedItem |
| 16 | metrics: busy time not accumulated | killed | MetricsTests.Timings_UseTheInjectedClock |
| 17 | metrics: simple worker does not count `In` | killed | MetricsTests.OnCompletion_EveryStageSatisfies_In_Equals_Out_Plus_Failed |
| 18 | order: reorder permit never released (window exhausted -> stall) | killed (timeout) | hang until the per-mutant timeout |
| 19 | order: next-expected ordinal never advances (stall) | killed (timeout) | hang until the per-mutant timeout |
| 20 | order: tombstones are emitted as default values | killed | FailureModeTests.Continue_DeadLettersFailures_AndDeliversTheRest |
| 21 | order: every item gets the same ordinal | killed (timeout) | hang until the per-mutant timeout |
| 22 | order: buffered metric also counts the head being drained | killed | GapTests.ReorderBufferMetric_DoesNotCountTheHeadThatIsAboutToBeEmitted |
| 23 | order: default window is 2x parallelism instead of 4x | killed (timeout) | hang until the per-mutant timeout |
| 24 | order: sink keeps `Ordered` (not forced off) | killed | PropertyTests.RandomPipelines_AlwaysAccountForEveryAcceptedItem |
| 25 | sink: delivered items are not removed from the pending set | killed | BackpressureTests.QueueDepthMetric_ReachesCapacity_WhenTheConsumerIsSlower |
| 26 | emit: queue write ignores run cancellation (blocked write never wakes) | killed (timeout) | hang until the per-mutant timeout |
| 27 | context: fatal error no longer cancels the run | killed (timeout) | hang until the per-mutant timeout |
| 28 | context: last fatal error wins instead of the first | killed | MutationFollowUpTests.FirstFatalError_IsTheOneReported_WhenASecondOneFollows |
| 29 | context: dead-lettered items stay in the pending set | killed | FailureModeTests.Continue_DeadLettersFailures_AndDeliversTheRest |
| 30 | context: dead-letter report cap off by one (`<` -> `<=`) | killed | RetryTests.ExternalDeadLetterSink_ReceivesEverything_ReportIsCapped_CountIsExact |
| 31 | context: accepted items are not registered as pending | killed | BackpressureTests.BoundedQueue_BlocksTheProducer_AtCapacityPlusWorkersPlusOne |
| 32 | context: pending sequences are not sorted before they are reported | SURVIVED |  |
| 33 | context: FailFast flag inverted | killed (timeout) | hang until the per-mutant timeout |
| 34 | pump: source failure under fail-fast does not cancel | killed (timeout) | hang until the per-mutant timeout |
| 35 | report: cancelled takes precedence over faulted | killed | FailureModeTests.FailFast_AbandonsItemsInFlightInOtherWorkers |
| 36 | builder: reserved stage name `sink` accepted | killed | OptionsTests.Stage_ReservesSinkName |
| 37 | batch: size flush at `>` instead of `>=` | killed (timeout) | hang until the per-mutant timeout |
| 38 | batch: timer waits twice as long | killed (timeout) | hang until the per-mutant timeout |
| 39 | batch: consumer leaving does not cancel the pending read (hang) | killed (timeout) | hang until the per-mutant timeout |
| 40 | merge: consumer leaving does not cancel the pumps (hang) | killed (timeout) | hang until the per-mutant timeout |
| 41 | merge: queue one bigger than requested | killed | MergeTests.SlowConsumer_BackpressuresEverySource_ToCapacityPlusSourcesPlusOne |

Not tested by mutation: `Fan.Merge`/`Fan.Batch` beyond the mutants in the table above and Stryker's run, the demo project, and anything inside Stryker's 87 "ignored" and 7 "no coverage" mutants.

## Known limits

* **In-process only.** No persistence, no distribution, no broker. If the process dies, every in-flight item is lost: delivery is **at-most-once on a crash** (and "not processed" after cancellation means "not confirmed", not "no side effect").
* **Stages are 1:1.** Every input yields exactly one output or one dead letter; there is no filter/flat-map stage (use `Fan.Batch` before the pipeline, or return a value that downstream ignores).
* **Cooperative only.** Cancellation and `ItemTimeout` rely on the stage function observing its token. A function that blocks forever without checking it will hold its worker (and `RunAsync`) forever; a source that ignores its cancellation token while blocked inside `MoveNextAsync` has the same effect.
* **Retries hold the worker** during backoff, and ordered stages have head-of-line blocking within their window.
* **Not TPL Dataflow, not Akka Streams.** No graph topologies (only a linear chain plus the `Fan` helpers), no completion propagation between independent blocks, no supervision strategies, no rate limiting or windowing beyond `Batch`.
* **Only tested on Windows locally** with .NET 10.0.402. The CI workflow targets Ubuntu but **CI not yet confirmed.**
* **Throughput is unmeasured.** The demo's records/s figures are a timer-bound toy, not a benchmark.

## Build

```
dotnet restore
dotnet build -warnaserror
dotnet test
```

`nuget.config` clears all package sources and adds nuget.org, so restore works regardless of the user-level NuGet configuration. `global.json` pins SDK 10.0.x (roll-forward to the latest feature band) and selects the Microsoft Testing Platform runner for `dotnet test`.

## Repository

```
git clone https://github.com/Supp3r-Man/dotnet-channels-pipeline.git
```

CI not yet confirmed.

## License

MIT, see [LICENSE](LICENSE).
