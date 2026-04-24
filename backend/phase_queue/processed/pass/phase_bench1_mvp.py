"""phase_bench1_mvp.py — Phase 2 MVP orchestrator (broad + admission + coverage).

Dropped into ``phase_queue/inbox/`` for the daemon. Runs in sequence:

  1. ``benchmark_builder.build_benchmarks`` → emits three JSON specs under
     ``benchmark/``.
  2. ``run_named_reaction_benchmark_small.py`` equivalent over the broad spec
     (by dynamically invoking the existing runner via subprocess with
     ``--benchmark named_reaction_benchmark_broad.json`` and report paths
     routed into the Phase 2 report dir).
  3. ``run_family_admission_benchmark.py`` → confusion matrix.
  4. ``run_corpus_coverage_benchmark.py`` → recall@1/@3/@5.

Writes a consolidated summary JSON:
  ``<report-dir>/<ts>/phase_bench1_mvp_summary.json``

Exit codes:
  0 — all three benchmarks ran and summary written.
  1 — benchmark_builder failed (DB/alias error).
  2 — at least one runner failed (partial summary still written).

Daemon invokes with::

    python phase_bench1_mvp.py \\
        --db <backend>/app/labint.db \\
        --report-dir <backend>/reports/family_completion_phase_bench1_mvp

but we consume only ``--db`` and ``--report-dir`` (daemon convention).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path


# bootstrap path to find backend root
_HERE = Path(__file__).resolve().parent
_cand = _HERE
_BACKEND = None
for _ in range(5):
    if (_cand / "final_state_verifier.py").exists():
        _BACKEND = _cand
        if str(_cand) not in sys.path:
            sys.path.insert(0, str(_cand))
        break
    _cand = _cand.parent
for _p in os.environ.get("PYTHONPATH", "").split(os.pathsep):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

assert _BACKEND is not None, "Could not locate backend root (final_state_verifier.py)"


def _run(cmd, cwd, log_file=None):
    """Run subprocess, capture + persist log, return (returncode, stdout_tail)."""
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(cwd) + (os.pathsep + existing if existing else "")
    print(f"[bench1]   $ {' '.join(cmd)}", flush=True)
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            env=env,
            timeout=3600,
        )
    except subprocess.TimeoutExpired as e:
        if log_file:
            log_file.write_text(f"[TIMEOUT] {e}\n", encoding="utf-8")
        return -1, "timeout"
    if log_file:
        log_file.write_text(
            (proc.stdout or "") + "\n--- STDERR ---\n" + (proc.stderr or ""),
            encoding="utf-8",
        )
    tail = (proc.stdout or "").splitlines()[-40:]
    return proc.returncode, "\n".join(tail)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--report-dir", required=True)
    ap.add_argument("--sample-per-family", type=int, default=2)
    ap.add_argument("--coverage-max-cases", type=int, default=0,
                    help="Cap for corpus coverage during first run (0 = all).")
    ns = ap.parse_args()

    db_path = Path(ns.db).resolve()
    now = dt.datetime.now()
    stamp = now.strftime("%Y%m%d_%H%M%S")
    out_dir = Path(ns.report_dir).resolve() / stamp
    out_dir.mkdir(parents=True, exist_ok=True)

    backend = _BACKEND
    benchmark_dir = backend / "benchmark"
    benchmark_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "phase_tag": "phase_bench1_mvp",
        "started_at": now.isoformat(),
        "db": str(db_path),
        "out_dir": str(out_dir),
        "stages": [],
    }

    # ---- Stage 1: builder ---------------------------------------------------
    try:
        import benchmark_builder  # noqa: E402
    except Exception as e:
        summary["error"] = f"builder import failed: {e}"
        summary["overall_ok"] = False
        (out_dir / "phase_bench1_mvp_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"[bench1] ERROR: {summary['error']}", file=sys.stderr)
        return 1

    try:
        build_stats = benchmark_builder.build_benchmarks(
            db_path=db_path,
            out_dir=benchmark_dir,
            sample_per_family=ns.sample_per_family,
        )
    except Exception as e:
        summary["error"] = f"builder failed: {e}"
        summary["overall_ok"] = False
        (out_dir / "phase_bench1_mvp_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"[bench1] ERROR: {summary['error']}", file=sys.stderr)
        return 1
    summary["stages"].append({"stage": "builder", "ok": True, "stats": build_stats})
    print(f"[bench1] builder ok: {build_stats}", flush=True)

    ok_all = True

    # ---- Stage 2: broad benchmark (existing runner) -------------------------
    broad_runner = backend / "run_named_reaction_benchmark_small.py"
    broad_cmd = [
        sys.executable,
        str(broad_runner),
        "--db", str(db_path),
        "--benchmark", str(benchmark_dir / "named_reaction_benchmark_broad.json"),
        "--csv-out", str(out_dir / "broad_results.csv"),
        "--json-out", str(out_dir / "broad_results.json"),
        "--report-md", str(out_dir / "broad_report.md"),
    ]
    code, tail = _run(broad_cmd, cwd=backend, log_file=out_dir / "broad_run.log")
    ok = (code == 0)
    ok_all = ok_all and ok
    # pull summary if present
    bsum = None
    bjson = out_dir / "broad_results.json"
    if bjson.exists():
        try:
            bsum = json.loads(bjson.read_text(encoding="utf-8")).get("summary")
        except Exception:
            bsum = None
    summary["stages"].append({
        "stage": "broad",
        "ok": ok,
        "exit_code": code,
        "summary": bsum,
        "tail": tail,
    })

    # ---- Stage 3: family admission ------------------------------------------
    adm_runner = backend / "run_family_admission_benchmark.py"
    adm_cmd = [
        sys.executable,
        str(adm_runner),
        "--db", str(db_path),
        "--benchmark", str(benchmark_dir / "family_admission_benchmark.json"),
        "--out-dir", str(out_dir),
    ]
    code, tail = _run(adm_cmd, cwd=backend, log_file=out_dir / "admission_run.log")
    ok = (code == 0)
    ok_all = ok_all and ok
    asum = None
    ajson = out_dir / "family_admission_results.json"
    if ajson.exists():
        try:
            asum = json.loads(ajson.read_text(encoding="utf-8")).get("summary")
        except Exception:
            asum = None
    summary["stages"].append({
        "stage": "admission",
        "ok": ok,
        "exit_code": code,
        "summary": asum,
        "tail": tail,
    })

    # ---- Stage 4: corpus coverage -------------------------------------------
    cov_runner = backend / "run_corpus_coverage_benchmark.py"
    cov_cmd = [
        sys.executable,
        str(cov_runner),
        "--db", str(db_path),
        "--benchmark", str(benchmark_dir / "corpus_coverage_benchmark.json"),
        "--out-dir", str(out_dir),
        "--max-cases", str(ns.coverage_max_cases),
    ]
    code, tail = _run(cov_cmd, cwd=backend, log_file=out_dir / "coverage_run.log")
    ok = (code == 0)
    ok_all = ok_all and ok
    csum = None
    cjson = out_dir / "corpus_coverage_results.json"
    if cjson.exists():
        try:
            csum = json.loads(cjson.read_text(encoding="utf-8")).get("summary")
        except Exception:
            csum = None
    summary["stages"].append({
        "stage": "coverage",
        "ok": ok,
        "exit_code": code,
        "summary": csum,
        "tail": tail,
    })

    # ---- finalize -----------------------------------------------------------
    summary["finished_at"] = dt.datetime.now().isoformat()
    summary["overall_ok"] = bool(ok_all)

    summary_path = out_dir / "phase_bench1_mvp_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[bench1] summary={summary_path}  overall_ok={ok_all}", flush=True)
    # Also print compact headline so daemon log has useful numbers.
    if bsum:
        print(f"[bench1] broad:     top1={bsum.get('top1_accuracy')} top3={bsum.get('top3_accuracy')} cases={bsum.get('total_cases')}")
    if asum:
        print(f"[bench1] admission: top1={asum.get('top1_accuracy')} top3={asum.get('top3_accuracy')} confused={asum.get('confused_pairs')}")
    if csum:
        print(f"[bench1] coverage:  r@1={csum.get('recall_at_1')} r@3={csum.get('recall_at_3')} r@5={csum.get('recall_at_5')} cases={csum.get('total_cases')}")

    return 0 if ok_all else 2


if __name__ == "__main__":
    sys.exit(main())
