#!/usr/bin/env python
"""Re-run Stryker.NET's surviving mutants against the FULL test suite.

Why: with the Microsoft Testing Platform runner, Stryker selects tests per mutant from a per-test coverage capture.
For async iterators and async state machines that capture is unreliable, and Stryker reported survivors that the
full suite kills (e.g. removing `DisarmTimer` after a size flush in Fan.Batch is killed by
SizeFlush_DisarmsTheTimer_SoNoSpuriousEmptyBatchFollows when the whole suite is run). This script takes the
`Survived` mutants from Stryker's mutation-report.json, applies each one textually at the reported location,
runs `dotnet test --no-restore` (all tests), and restores the file.

Usage (repo root, dotnet on PATH):
    python scripts/recheck_stryker_survivors.py <path-to-mutation-report.json> [out.json]
Env: MUTANT_TIMEOUT seconds per mutant (default 90). Same safety rules as mutation_check.py (backup + byte compare,
restore in finally, timed-out runs killed by process tree of the process this script started).
"""
import filecmp
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mutation_check import run_tests, short  # noqa: E402


def apply(text, loc, replacement):
    lines = text.split("\n")
    offsets = [0]
    for line in lines:
        offsets.append(offsets[-1] + len(line) + 1)
    sl, sc = loc["start"]["line"], loc["start"]["column"]
    el, ec = loc["end"]["line"], loc["end"]["column"]
    start = offsets[sl - 1] + sc - 1
    end = offsets[el - 1] + ec - 1
    return text[:start] + replacement + text[end:], text[start:end]


def main():
    report = json.load(open(sys.argv[1], encoding="utf-8"))
    out_path = sys.argv[2] if len(sys.argv) > 2 else None
    timeout = int(os.environ.get("MUTANT_TIMEOUT", "90"))
    os.makedirs(".tools", exist_ok=True)
    backup = tempfile.mkdtemp(prefix="src-backup-", dir=".tools")
    shutil.rmtree(backup)
    shutil.copytree("src", backup, ignore=shutil.ignore_patterns("bin", "obj"))

    status, _, secs = run_tests(timeout * 3)
    if status != "passed":
        sys.exit("baseline is not green")

    results = []
    originals = {}
    try:
        for full_path, fd in report["files"].items():
            name = full_path.replace("\\", "/").split("/")[-1]
            path = "src/ChannelsPipeline/" + name
            with open(path, "rb") as fh:
                raw = fh.read()
            originals.setdefault(path, raw)
            text = raw.decode("utf-8").replace("\r\n", "\n")
            if text != fd["source"].replace("\r\n", "\n"):
                print("WARNING: %s differs from the source Stryker mutated; skipping" % name)
                continue
            for m in fd["mutants"]:
                if m["status"] != "Survived":
                    continue
                mutated, original_span = apply(text, m["location"], m["replacement"])
                try:
                    with open(path, "w", encoding="utf-8", newline="\n") as fh:
                        fh.write(mutated)
                    status, tests, secs = run_tests(timeout)
                finally:
                    with open(path, "wb") as fh:
                        fh.write(raw)
                verdict = {"failed": "killed by full suite", "timeout": "killed by full suite (timeout)", "passed": "SURVIVED",
                           "compile-error": "INVALID (no compile)"}[status]
                row = {"id": m["id"], "file": name, "line": m["location"]["start"]["line"], "mutator": m["mutatorName"],
                       "original": original_span, "replacement": m["replacement"], "verdict": verdict, "tests": short(tests)}
                results.append(row)
                print("%s:%d [%s] %r -> %r : %s %s (%.0fs)" % (name, row["line"], row["mutator"], original_span[:40],
                                                               m["replacement"][:40], verdict, row["tests"], secs), flush=True)
    finally:
        for path, raw in originals.items():
            with open(path, "wb") as fh:
                fh.write(raw)

    def same(d):
        if d.left_only or d.right_only or d.diff_files or d.funny_files:
            return False
        return all(same(s) for s in d.subdirs.values())

    assert same(filecmp.dircmp("src", backup, ignore=["bin", "obj"])), "src/ differs from the backup after the run"
    print("\nsrc/ is byte-identical to the backup taken before the run.")
    shutil.rmtree(backup, ignore_errors=True)
    killed = sum(1 for r in results if r["verdict"].startswith("killed"))
    print("re-checked %d Stryker survivors: %d killed by the full suite, %d truly survived, %d invalid" % (
        len(results), killed, sum(1 for r in results if r["verdict"] == "SURVIVED"),
        sum(1 for r in results if r["verdict"].startswith("INVALID"))))
    if out_path:
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(results, fh, indent=1)


if __name__ == "__main__":
    main()
