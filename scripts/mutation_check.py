#!/usr/bin/env python
"""Hand-rolled mutation check, used to cover the regions Stryker.NET could not mutate (see README).

Applies one behaviour-breaking textual replacement at a time to src/, runs `dotnet test --no-restore`, restores the
file. A mutant is "killed" if the test run fails (or hangs until the per-mutant timeout), "SURVIVED" if everything
passes, "INVALID" if the pattern did not match exactly once or the mutant did not compile.

Usage (from the repo root, with dotnet on PATH):
    python scripts/mutation_check.py              # all mutants
    python scripts/mutation_check.py 3 7 12       # only these numbers
Environment: MUTANT_TIMEOUT seconds per mutant (default 60).

Safety: src/ is copied to .tools/ before the run and compared byte-for-byte afterwards; every file is restored in a
finally block. A timed-out run is killed with `taskkill /T` (or SIGKILL of the process group elsewhere) for the process
tree rooted at the dotnet process THIS script started, never by image name, so other dotnet work on the machine is untouched.
"""
import filecmp
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

SRC = "src/ChannelsPipeline/"
STAGE = SRC + "StageRunner.cs"
CTX = SRC + "RunContext.cs"
PIPE = SRC + "Pipeline.cs"
FAN = SRC + "Fan.cs"
OPTS = SRC + "Options.cs"

# (description, file, old text, new text). `old` must occur exactly once in the file.
MUTATIONS = [
    # ---- retry loop (Stryker's compile-recovery "safe mode" skips all of ProcessAsync)
    ("retry: one extra attempt (`<` -> `<=`)", STAGE,
     "if (attempt < retry.MaxAttempts && retry.ShouldRetry(failure))", "if (attempt <= retry.MaxAttempts && retry.ShouldRetry(failure))"),
    ("retry: one attempt too few", STAGE,
     "if (attempt < retry.MaxAttempts && retry.ShouldRetry(failure))", "if (attempt < retry.MaxAttempts - 1 && retry.ShouldRetry(failure))"),
    ("retry: ShouldRetry predicate ignored", STAGE,
     "if (attempt < retry.MaxAttempts && retry.ShouldRetry(failure))", "if (attempt < retry.MaxAttempts)"),
    ("retry: retried-counter not incremented", STAGE,
     "                    Interlocked.Increment(ref _retried);\n", "                    Interlocked.Add(ref _retried, 0);\n"),
    ("retry: backoff always uses the first attempt's delay", STAGE,
     "var delay = retry.Backoff(attempt);", "var delay = retry.Backoff(1);"),
    ("retry: backoff wait skipped", STAGE,
     "if (delay > TimeSpan.Zero) await Task.Delay(delay, _ctx.Time, token)", "if (delay < TimeSpan.Zero) await Task.Delay(delay, _ctx.Time, token)"),
    ("dead letter: attempt count off by one", STAGE,
     "new DeadLetter(env.Seq, _name, env.Value, attempt, failure)", "new DeadLetter(env.Seq, _name, env.Value, attempt + 1, failure)"),
    ("dead letter: failed-counter not incremented", STAGE,
     "                Interlocked.Increment(ref _failed);\n", "                Interlocked.Add(ref _failed, 0);\n"),
    ("fail-fast: exhausted item no longer fails the run", STAGE,
     "if (_ctx.FailFast) _ctx.Fail(new StageFailedException(", "if (_ctx.FailFast && false) _ctx.Fail(new StageFailedException("),
    ("fail-fast: StageFailedException reports attempt 1", STAGE,
     "new StageFailedException(_name, env.Seq, attempt, failure)", "new StageFailedException(_name, env.Seq, 1, failure)"),
    ("retry loop: no cancellation check before each attempt", STAGE,
     "attempt++;\n                token.ThrowIfCancellationRequested();", "attempt++;"),
    ("retry loop: no 'abandon item if cancelled' check after a failure", STAGE,
     "                token.ThrowIfCancellationRequested();\n\n                if (attempt < retry.MaxAttempts", "                if (attempt < retry.MaxAttempts"),
    ("retry loop: run cancellation treated as an item failure", STAGE,
     "catch (OperationCanceledException) when (token.IsCancellationRequested)\n                {\n                    throw;\n                }\n                catch (Exception ex)\n                {\n                    failure = ex;",
     "catch (Exception ex)\n                {\n                    failure = ex;"),
    ("timeout: wrong exception type (rethrows OperationCanceledException)", STAGE,
     "throw new ItemTimeoutException(_name, timeout);", "throw;"),
    ("metrics: busy-workers counter never decremented", STAGE,
     "            Interlocked.Decrement(ref _busy);\n", ""),
    ("metrics: busy time not accumulated", STAGE,
     "            Interlocked.Add(ref _busyTicks, _ctx.Time.GetElapsedTime(started).Ticks);\n", "            Interlocked.Add(ref _busyTicks, 0);\n"),
    ("metrics: simple worker does not count `In`", STAGE,
     "if (!_input.Reader.TryRead(out var env)) continue;\n                Interlocked.Increment(ref _in);", "if (!_input.Reader.TryRead(out var env)) continue;"),
    # ---- ordered stage
    ("order: reorder permit never released (window exhausted -> stall)", STAGE,
     "            _window!.Release();\n", ""),
    ("order: next-expected ordinal never advances (stall)", STAGE,
     "                _nextEmit++;\n", "                _nextEmit += 0;\n"),
    ("order: tombstones are emitted as default values", STAGE,
     "if (next.HasValue) await EmitAsync(next.Seq, next.Value!).ConfigureAwait(false);", "await EmitAsync(next.Seq, next.Value!).ConfigureAwait(false);"),
    ("order: every item gets the same ordinal", STAGE,
     "ordinal = _nextOrdinal++;", "ordinal = _nextOrdinal++ * 0;"),
    ("order: buffered metric also counts the head being drained", STAGE,
     "var waiting = _buffer.Count - (becomeDrainer ? 1 : 0);", "var waiting = _buffer.Count;"),
    ("order: default window is 2x parallelism instead of 4x", OPTS,
     "ReorderWindow ?? checked(4 * Parallelism)", "ReorderWindow ?? checked(2 * Parallelism)"),
    ("order: sink keeps `Ordered` (not forced off)", PIPE,
     "_options.SinkOptions with { Ordered = false }", "_options.SinkOptions"),
    # ---- emit / delivery accounting
    ("sink: delivered items are not removed from the pending set", STAGE,
     "            _ctx.MarkDelivered(seq);\n", ""),
    ("emit: queue write ignores run cancellation (blocked write never wakes)", STAGE,
     "await _output.WriteAsync(new Envelope<TOut>(seq, value), _ctx.Token)", "await _output.WriteAsync(new Envelope<TOut>(seq, value), CancellationToken.None)"),
    # ---- run context
    ("context: fatal error no longer cancels the run", CTX,
     "if (cancel) _cts.Cancel();", "if (cancel && false) _cts.Cancel();"),
    ("context: last fatal error wins instead of the first", CTX,
     "Interlocked.CompareExchange(ref _fatal, error, null);", "Volatile.Write(ref _fatal, error);"),
    ("context: dead-lettered items stay in the pending set", CTX,
     "            _pending.TryRemove(deadLetter.Sequence, out _);\n", ""),
    ("context: dead-letter report cap off by one (`<` -> `<=`)", CTX,
     "if (_deadLetters.Count < Options.MaxReportedDeadLetters)", "if (_deadLetters.Count <= Options.MaxReportedDeadLetters)"),
    ("context: accepted items are not registered as pending", CTX,
     "        _pending[seq] = 0;\n", ""),
    ("context: pending sequences are not sorted before they are reported", CTX,
     "        list.Sort();\n", ""),
    ("context: FailFast flag inverted", CTX,
     "Options.FailureMode == FailureMode.FailFast", "Options.FailureMode != FailureMode.FailFast"),
    # ---- pipeline
    ("pump: source failure under fail-fast does not cancel", PIPE,
     "cancel: ctx.FailFast);", "cancel: false);"),
    ("report: cancelled takes precedence over faulted", PIPE,
     "ctx.Fatal is not null ? RunOutcome.Faulted : ctx.Aborted ? RunOutcome.Cancelled : RunOutcome.Completed",
     "ctx.Aborted ? RunOutcome.Cancelled : ctx.Fatal is not null ? RunOutcome.Faulted : RunOutcome.Completed"),
    ("builder: reserved stage name `sink` accepted", PIPE,
     'if (name == "sink" || _names.Contains(name))', "if (_names.Contains(name))"),
    # ---- fan helpers
    ("batch: size flush at `>` instead of `>=`", FAN,
     "if (batch.Count >= size)", "if (batch.Count > size)"),
    ("batch: timer waits twice as long", FAN,
     "timer = Task.Delay(maxWait, time, timerCts.Token);", "timer = Task.Delay(maxWait * 2, time, timerCts.Token);"),
    ("batch: consumer leaving does not cancel the pending read (hang)", FAN,
     "        finally\n        {\n            linked.Cancel();", "        finally\n        {"),
    ("merge: consumer leaving does not cancel the pumps (hang)", FAN,
     "        finally\n        {\n            cts.Cancel();\n            await closer", "        finally\n        {\n            await closer"),
    ("merge: queue one bigger than requested", FAN,
     "new BoundedChannelOptions(capacity) { SingleReader = true }", "new BoundedChannelOptions(capacity + 1) { SingleReader = true }"),
]


def kill_tree(proc):
    """Kill the process tree rooted at a process this script started."""
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"], capture_output=True)
    else:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def run_tests(timeout):
    """Returns (status, failing tests, seconds). status: passed | failed | compile-error | timeout."""
    cmd = ["dotnet", "test", "tests/ChannelsPipeline.Tests", "--no-restore"]
    kwargs = {} if os.name == "nt" else {"start_new_session": True}
    started = time.time()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, **kwargs)
    try:
        out, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        kill_tree(proc)
        out, _ = proc.communicate()
        return "timeout", [], time.time() - started
    elapsed = time.time() - started
    if proc.returncode == 0:
        return "passed", [], elapsed
    if re.search(r"error CS\d+", out):
        return "compile-error", [], elapsed
    tests = sorted(set(re.findall(r"^failed (\S+)", out, flags=re.M)))
    return "failed", tests, elapsed


def short(tests):
    names = [t.replace("ChannelsPipeline.Tests.", "").split("(")[0] for t in tests]
    names = sorted(set(names))
    return ", ".join(names[:3]) + (" (+%d more)" % (len(names) - 3) if len(names) > 3 else "")


def main():
    wanted = {int(a) for a in sys.argv[1:]}
    timeout = int(os.environ.get("MUTANT_TIMEOUT", "60"))
    os.makedirs(".tools", exist_ok=True)
    backup = tempfile.mkdtemp(prefix="src-backup-", dir=".tools")
    shutil.rmtree(backup)
    shutil.copytree("src", backup, ignore=shutil.ignore_patterns("bin", "obj"))
    print("backup of src/ at", backup, flush=True)

    status, _, secs = run_tests(timeout * 3)
    if status != "passed":
        sys.exit("baseline is not green (%s); fix that first" % status)
    print("baseline green in %.1fs" % secs, flush=True)

    originals = {}
    rows = []
    try:
        for i, (desc, path, old, new) in enumerate(MUTATIONS, 1):
            if wanted and i not in wanted:
                continue
            with open(path, "rb") as fh:
                raw = fh.read()
            originals.setdefault(path, raw)
            text = raw.decode("utf-8").replace("\r\n", "\n")
            if text.count(old) != 1:
                rows.append((i, desc, "INVALID (pattern count %d)" % text.count(old), ""))
                print("#%d INVALID pattern count %d" % (i, text.count(old)), flush=True)
                continue
            try:
                with open(path, "w", encoding="utf-8", newline="\n") as fh:
                    fh.write(text.replace(old, new))
                status, tests, secs = run_tests(timeout)
            finally:
                with open(path, "wb") as fh:
                    fh.write(raw)
            verdict = {"failed": "killed", "timeout": "killed (timeout)", "passed": "SURVIVED",
                       "compile-error": "INVALID (no compile)"}[status]
            names = short(tests) if tests else ("hang: test timed out after %ds" % timeout if status == "timeout" else "")
            rows.append((i, desc, verdict, names))
            print("#%d %s -> %s %s (%.0fs)" % (i, desc, verdict, names, secs), flush=True)
    finally:
        for path, raw in originals.items():
            with open(path, "wb") as fh:
                fh.write(raw)

    # source must equal the backup (bin/obj excluded)
    cmp = filecmp.dircmp("src", backup, ignore=["bin", "obj"])
    def same(d):
        if d.left_only or d.right_only or d.diff_files or d.funny_files:
            return False
        return all(same(s) for s in d.subdirs.values())
    assert same(cmp), "src/ differs from the backup after the run"
    print("\nsrc/ is byte-identical to the backup taken before the run.\n")

    print("| # | Mutation | Result | Failing test(s) |")
    print("|---|---|---|---|")
    for i, desc, verdict, names in rows:
        print("| %d | %s | %s | %s |" % (i, desc.replace("|", "/"), verdict, names.replace("|", "/")))
    valid = [r for r in rows if not r[2].startswith("INVALID")]
    killed = sum(1 for r in valid if r[2].startswith("killed"))
    timeouts = sum(1 for r in valid if r[2] == "killed (timeout)")
    print("\nkilled %d / %d valid mutants (%d of the kills were timeouts); invalid: %d" % (killed, len(valid), timeouts, len(rows) - len(valid)))
    shutil.rmtree(backup, ignore_errors=True)


if __name__ == "__main__":
    main()
