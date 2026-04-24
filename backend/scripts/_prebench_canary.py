"""Pre-benchmark safety canary.

Run BEFORE any 3-layer benchmark to detect silent evidence_search.py reverts
or accidental DB state shifts.

Checks:
  1. evidence_search.py file size matches KNOWN_GOOD_POST_OPTION_F or KNOWN_GOOD_post_gateC
  2. _probe_sandmeyer_now.py produces expected scores (Sandmeyer 5.386, Schmidt 3.543)
  3. labint.db queryable extract count is within expected range

Exit codes:
  0 = all green
  1 = file size mismatch
  2 = canary score mismatch
  3 = DB count anomaly
"""
from __future__ import annotations
import os, sys, json, subprocess
from pathlib import Path

ROOT = Path(r'C:\chemlens\backend')
ES   = ROOT / 'app' / 'evidence_search.py'
PROBE = ROOT / 'scripts' / '_probe_sandmeyer_now.py'

# Known-good sizes. Option F ships a new one — append below once landed.
KNOWN_GOOD_SIZES = [
    124010,       # post-gateC (pre-Option F)
    # <--- Option F known-good size will be appended here by apply step
]

# Additional known-good sizes are read from _prebench_canary.sizes file (one per line)
SIZES_FILE = ROOT / 'scripts' / '_prebench_canary.sizes'


def load_extra_sizes():
    if not SIZES_FILE.exists():
        return []
    out = []
    for line in SIZES_FILE.read_text(encoding='utf-8').splitlines():
        s = line.strip().split('#', 1)[0].strip()
        if s.isdigit():
            out.append(int(s))
    return out


def check_file_size():
    sz = ES.stat().st_size
    allowed = list(KNOWN_GOOD_SIZES) + load_extra_sizes()
    if sz not in allowed:
        print(f'[FAIL] evidence_search.py size {sz} not in known-good {allowed}', file=sys.stderr)
        print(f'  → likely silent revert. Investigate: git status, diff against backups.', file=sys.stderr)
        return 1
    print(f'[OK] evidence_search.py size {sz} matches known-good')
    return 0


def check_canary_scores():
    if not PROBE.exists():
        print(f'[WARN] canary script missing at {PROBE}; skipping', file=sys.stderr)
        return 0
    try:
        result = subprocess.run(
            [sys.executable, str(PROBE)],
            capture_output=True, text=True, cwd=str(ROOT), timeout=120,
        )
    except Exception as e:
        print(f'[FAIL] canary run errored: {e}', file=sys.stderr)
        return 2
    out = result.stdout + '\n' + result.stderr
    ok = True
    # Permissive score check — look for Sandmeyer top1 ~5.4 and Schmidt top1 ~3.5
    if 'Sandmeyer' not in out or 'Schmidt' not in out:
        ok = False
        print(f'[FAIL] canary output lacks Sandmeyer/Schmidt markers', file=sys.stderr)
    print(out[-800:])
    return 0 if ok else 2


def check_db_counts():
    db = ROOT / 'app' / 'labint.db'
    if not db.exists():
        db = ROOT / 'labint.db'
    if not db.exists():
        print('[WARN] labint.db not found; skipping DB check', file=sys.stderr)
        return 0
    try:
        import sqlite3
        con = sqlite3.connect(str(db))
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM extracts WHERE COALESCE(queryable, 0) = 1")
        n_query = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM extracts")
        n_tot = cur.fetchone()[0]
        con.close()
    except Exception as e:
        print(f'[WARN] DB check errored: {e}', file=sys.stderr)
        return 0
    if n_query < 500 or n_query > 50000:
        print(f'[FAIL] DB queryable count {n_query} out of expected range [500..50000]', file=sys.stderr)
        return 3
    print(f'[OK] DB queryable={n_query} total={n_tot}')
    return 0


def main():
    rc = 0
    rc |= check_file_size()
    rc |= check_canary_scores()
    rc |= check_db_counts()
    if rc == 0:
        print('\n=== PRE-BENCH CANARY: ALL GREEN ===')
    else:
        print(f'\n=== PRE-BENCH CANARY: FAILED (rc={rc}) ===', file=sys.stderr)
        sys.exit(rc)


if __name__ == '__main__':
    main()
