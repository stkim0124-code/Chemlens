"""rdkit_daemon.py — One-time-launch background runner for CHEMLENS phase
automation.

What this does
--------------
Polls ``chemlens/backend/phase_queue/inbox/`` for new ``*.py`` phase apply
scripts. For each one discovered:

  1. Move it into ``phase_queue/processing/`` (prevents double-processing).
  2. Run it via ``python <script> <default_args>`` in the current (user's)
     Python environment — the one that has RDKit installed.
  3. Capture stdout / stderr / exit code.
  4. If the script succeeds, also run ``final_state_verifier.py`` +
     ``family_completion_dashboard.py`` to refresh metrics.
  5. Write a machine-readable result JSON into ``phase_queue/results/``.
  6. Move the script into ``phase_queue/processed/<pass|fail>/``.

When the loop has processed one round (all inbox items drained), it sleeps
``--poll-interval`` seconds and repeats.

Stop conditions
---------------
*   ``Ctrl-C`` at any time.
*   A per-family ``hotfix`` streak hits ``--fail-streak`` (default 3)
    consecutively — daemon prints a loud warning but keeps running; the
    stopping is enforced by whoever is generating scripts (Claude), which
    reads the result JSONs and stops emitting new scripts.

Usage
-----
From the user's anaconda prompt::

    (rdkit) > cd chemlens\\backend
    (rdkit) > python rdkit_daemon.py

Leave the window minimised. That is the only interaction required per
phase — the daemon handles apply + verify + dashboard for every phase
script dropped into ``inbox/``.

Options::

    --db            path to labint.db  (default: app/labint.db)
    --inbox         override inbox dir
    --poll-interval seconds between polls (default: 5)
    --fail-streak   hotfix streak alert threshold (default: 3)
    --dry-run       list-but-don't-run mode
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


DEFAULT_DB = "app/labint.db"
DEFAULT_VERIFIER = "final_state_verifier.py"
DEFAULT_DASHBOARD = "family_completion_dashboard.py"
QUEUE_ROOT = "phase_queue"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _now_stamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def _ensure_tree(root: Path) -> dict[str, Path]:
    paths = {
        "inbox": root / "inbox",
        "processing": root / "processing",
        "processed_pass": root / "processed" / "pass",
        "processed_fail": root / "processed" / "fail",
        "results": root / "results",
        "logs": root / "logs",
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    return paths


def _list_inbox(inbox: Path) -> list[Path]:
    return sorted([p for p in inbox.iterdir() if p.is_file() and p.suffix == ".py"])


def _derive_phase_tag(script_path: Path) -> str:
    """``complete_family_evidence_phase16_v1.py`` -> ``phase16_v1``."""
    stem = script_path.stem
    if "phase" in stem:
        return stem[stem.index("phase"):]
    return stem


def _derive_default_args(script_path: Path, backend_dir: Path) -> list[str]:
    """Infer the args a phase apply script typically takes.

    Most phase scripts in this repo accept ``--db`` and ``--report-dir``. We
    pass both by default. If the user's script has a different CLI, they can
    override by adding a ``# daemon-args: ...`` line at the top.
    """
    # look for a directive on first 10 lines
    try:
        lines = script_path.read_text(encoding="utf-8", errors="replace").splitlines()[:20]
    except Exception:
        lines = []
    for ln in lines:
        if ln.strip().startswith("# daemon-args:"):
            return ln.split(":", 1)[1].strip().split()

    phase_tag = _derive_phase_tag(script_path)
    report_dir = backend_dir / "reports" / f"family_completion_{phase_tag}"
    return [
        "--db", str(backend_dir / DEFAULT_DB),
        "--report-dir", str(report_dir),
    ]


def _run_cmd(cmd: list[str], cwd: Path, log_path: Path, timeout: int = 1800) -> dict:
    """Run cmd, stream output into log_path, return a result dict.

    Prepends ``cwd`` (backend_dir) to PYTHONPATH so scripts executed from
    ``phase_queue/processing/`` can still ``import smiles_guard`` and any
    other backend-level helper. Without this, Python only adds the script's
    own directory (processing/) to sys.path.
    """
    start = time.time()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(cwd) + (os.pathsep + existing if existing else "")
    with open(log_path, "wb") as lf:
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(cwd),
                stdout=lf,
                stderr=subprocess.STDOUT,
                timeout=timeout,
                check=False,
                env=env,
            )
            code = proc.returncode
        except subprocess.TimeoutExpired:
            code = -1
            lf.write(f"\n[TIMEOUT after {timeout}s]\n".encode())
    dur = time.time() - start
    return {"cmd": cmd, "exit_code": code, "duration_sec": round(dur, 2), "log": str(log_path)}


def _write_result(results_dir: Path, script_path: Path, result: dict) -> Path:
    out = results_dir / f"{script_path.stem}.result.json"
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


def _process_one(
    script: Path,
    backend_dir: Path,
    paths: dict,
    db_path: Path,
    verifier: Path,
    dashboard: Path,
    dry_run: bool,
) -> dict:
    phase_tag = _derive_phase_tag(script)
    stamp = _now_stamp()
    log_path = paths["logs"] / f"{phase_tag}_{stamp}.log"

    print(f"[daemon] picking up: {script.name}  (phase={phase_tag})", flush=True)

    # 1) move into processing
    processing_path = paths["processing"] / script.name
    if dry_run:
        processing_path = script
        print(f"[daemon] DRY-RUN: would move to {processing_path}", flush=True)
    else:
        shutil.move(str(script), str(processing_path))

    result: dict = {
        "script": script.name,
        "phase_tag": phase_tag,
        "started_at": dt.datetime.now().isoformat(),
        "stages": [],
    }

    # 2) apply
    args = _derive_default_args(processing_path, backend_dir)
    apply_cmd = [sys.executable, str(processing_path)] + args
    if dry_run:
        print(f"[daemon] DRY-RUN apply cmd: {' '.join(apply_cmd)}", flush=True)
        apply_res = {"cmd": apply_cmd, "exit_code": 0, "duration_sec": 0.0, "dry_run": True}
    else:
        apply_res = _run_cmd(apply_cmd, backend_dir, log_path)
    apply_res["stage"] = "apply"
    result["stages"].append(apply_res)
    apply_ok = apply_res["exit_code"] == 0

    # 3) verifier + dashboard (only if apply passed)
    verify_ok = None
    dashboard_ok = None
    if apply_ok:
        vlog = paths["logs"] / f"{phase_tag}_{stamp}_verifier.log"
        vres = _run_cmd(
            [sys.executable, str(verifier), "--db", str(db_path)],
            backend_dir,
            vlog,
        ) if not dry_run else {"cmd": ["dry-run verifier"], "exit_code": 0, "duration_sec": 0.0}
        vres["stage"] = "verifier"
        result["stages"].append(vres)
        verify_ok = vres["exit_code"] == 0

        dlog = paths["logs"] / f"{phase_tag}_{stamp}_dashboard.log"
        # NOTE: family_completion_dashboard.py takes --backend-root (not --db).
        # It reads the latest final_state_verifier JSON from reports/ and
        # derives state from there, so we just hand it the backend root.
        dres = _run_cmd(
            [sys.executable, str(dashboard), "--backend-root", str(backend_dir)],
            backend_dir,
            dlog,
        ) if not dry_run else {"cmd": ["dry-run dashboard"], "exit_code": 0, "duration_sec": 0.0}
        dres["stage"] = "dashboard"
        result["stages"].append(dres)
        dashboard_ok = dres["exit_code"] == 0

    result["apply_ok"] = apply_ok
    result["verify_ok"] = verify_ok
    result["dashboard_ok"] = dashboard_ok
    result["finished_at"] = dt.datetime.now().isoformat()
    result["overall_ok"] = bool(apply_ok and (verify_ok is None or verify_ok) and (dashboard_ok is None or dashboard_ok))

    # 4) write result, move processed
    if not dry_run:
        _write_result(paths["results"], processing_path, result)
        target = paths["processed_pass"] if result["overall_ok"] else paths["processed_fail"]
        shutil.move(str(processing_path), str(target / processing_path.name))

    status_tag = "PASS" if result["overall_ok"] else "FAIL"
    print(f"[daemon] {script.name} -> {status_tag}  apply={apply_ok} verify={verify_ok} dash={dashboard_ok}", flush=True)
    return result


# ---------------------------------------------------------------------------
# main loop
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--inbox", default=None)
    ap.add_argument("--verifier", default=DEFAULT_VERIFIER)
    ap.add_argument("--dashboard", default=DEFAULT_DASHBOARD)
    ap.add_argument("--poll-interval", type=int, default=5)
    ap.add_argument("--fail-streak", type=int, default=3)
    ap.add_argument("--dry-run", action="store_true")
    ns = ap.parse_args()

    backend_dir = Path(__file__).parent.resolve()
    db_path = (backend_dir / ns.db).resolve()
    verifier_path = (backend_dir / ns.verifier).resolve()
    dashboard_path = (backend_dir / ns.dashboard).resolve()
    queue_root = Path(ns.inbox).resolve() if ns.inbox else (backend_dir / QUEUE_ROOT)

    paths = _ensure_tree(queue_root)
    print(f"[daemon] starting. backend_dir={backend_dir}", flush=True)
    print(f"[daemon] db={db_path}  verifier={verifier_path.name}  dashboard={dashboard_path.name}", flush=True)
    print(f"[daemon] queue={queue_root}  poll={ns.poll_interval}s  fail-streak-alert={ns.fail_streak}", flush=True)
    if ns.dry_run:
        print("[daemon] DRY-RUN: nothing will be moved or executed", flush=True)

    # keep a rolling log of per-family hotfix streaks
    fail_streak: dict[str, int] = {}

    try:
        while True:
            items = _list_inbox(paths["inbox"])
            if items:
                for script in items:
                    res = _process_one(
                        script=script,
                        backend_dir=backend_dir,
                        paths=paths,
                        db_path=db_path,
                        verifier=verifier_path,
                        dashboard=dashboard_path,
                        dry_run=ns.dry_run,
                    )
                    # streak tracking by phase_tag prefix family
                    key = res["phase_tag"]
                    if res["overall_ok"]:
                        fail_streak.pop(key, None)
                    else:
                        fail_streak[key] = fail_streak.get(key, 0) + 1
                        if fail_streak[key] >= ns.fail_streak:
                            print(
                                f"[daemon] !! WARNING: {key} has failed {fail_streak[key]} times in a row. "
                                f"Generator (Claude) is expected to halt; daemon stays up for recovery scripts.",
                                flush=True,
                            )
            else:
                time.sleep(ns.poll_interval)
    except KeyboardInterrupt:
        print("\n[daemon] interrupted by user. bye.", flush=True)
        return 0


if __name__ == "__main__":
    sys.exit(main())
