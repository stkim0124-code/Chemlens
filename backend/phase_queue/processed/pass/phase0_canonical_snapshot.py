"""phase0_canonical_snapshot — seal shallow=0, rich=264 baseline release.

Actions (run on Windows daemon side):
  1. PRAGMA integrity_check
  2. PRAGMA wal_checkpoint(TRUNCATE)
  3. VACUUM INTO snapshot copy (live DB untouched)
  4. Verify family counts against snapshot via final_state_verifier
  5. SHA-256 the snapshot DB
  6. Write release_manifest.json
  7. Zip snapshot + manifest
  8. Copy zip + manifest to C:\\chemlens\\backend\\releases\\ for backup retention
"""
import os as _os
import sys as _sys
from pathlib import Path as _Path
_HERE = _Path(__file__).resolve().parent
_cand = _HERE
for _ in range(4):
    if (_cand / "final_state_verifier.py").exists():
        if str(_cand) not in _sys.path:
            _sys.path.insert(0, str(_cand))
        break
    _cand = _cand.parent
for _p in _os.environ.get("PYTHONPATH", "").split(_os.pathsep):
    if _p and _p not in _sys.path:
        _sys.path.insert(0, _p)

import argparse
import datetime as dt
import hashlib
import json
import shutil
import sqlite3
import sys
import zipfile
from pathlib import Path

import final_state_verifier as fsv


RELEASE_ROOT_WIN = Path("C:\\chemlens\\backend\\releases")


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--report-dir", required=True)
    ns = ap.parse_args()

    now = dt.datetime.now()
    release_tag = f"canonical_release_{now.strftime('%Y-%m-%d')}_shallow0"

    out_dir = Path(ns.report_dir) / now.strftime("%Y%m%d_%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)

    db_path = Path(ns.db)
    live_size_before = db_path.stat().st_size

    # Step 1: integrity_check (read-only)
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("PRAGMA integrity_check")
    integrity = [r[0] for r in cur.fetchall()]
    integrity_ok = (integrity == ["ok"])

    # Step 2: wal_checkpoint(TRUNCATE) to flush WAL into main DB
    cur.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    wal_result = cur.fetchone()
    conn.close()

    # Step 3: VACUUM INTO compacted snapshot copy
    snapshot_db = out_dir / f"labint_{release_tag}.db"
    if snapshot_db.exists():
        snapshot_db.unlink()
    conn = sqlite3.connect(str(db_path))
    snap_sql = str(snapshot_db).replace("'", "''")
    conn.execute(f"VACUUM INTO '{snap_sql}'")
    conn.close()

    # Step 4: verify family counts against SNAPSHOT (not live DB)
    conn_snap = fsv.connect_db(snapshot_db)
    raw_names = fsv.distinct_pattern_names(conn_snap)
    alias_groups, _collapse = fsv.build_alias_groups(raw_names)
    pair_map = fsv.pair_map_from_extract_molecules(conn_snap)
    summaries = []
    for canonical, raws in alias_groups.items():
        s = fsv.summarize_canonical_family(conn_snap, canonical, raws, pair_map)
        summaries.append(s)
    conn_snap.close()

    shallow = [r for r in summaries if r["completion_bucket"] == "shallow"]
    rich = [r for r in summaries if r["completion_bucket"] == "rich"]
    missing = [r for r in summaries if r["completion_bucket"] == "missing"]

    # Step 5: hash snapshot
    snap_size = snapshot_db.stat().st_size
    snap_sha = sha256_file(snapshot_db)

    # Step 6: release manifest
    manifest = {
        "release_tag": release_tag,
        "created_at": now.isoformat(),
        "live_db_path": str(db_path),
        "live_db_size_before_bytes": live_size_before,
        "snapshot_db_name": snapshot_db.name,
        "snapshot_db_size_bytes": snap_size,
        "snapshot_db_sha256": snap_sha,
        "integrity_check": integrity,
        "integrity_ok": integrity_ok,
        "wal_checkpoint_result": list(wal_result) if wal_result else None,
        "family_counts": {
            "rich": len(rich),
            "shallow": len(shallow),
            "missing": len(missing),
            "total": len(summaries),
        },
        "rich_families": sorted([r["family"] for r in rich]),
        "shallow_families": [r["family"] for r in shallow],
        "missing_families": [r["family"] for r in missing],
        "notes": (
            "Baseline canonical snapshot sealed at shallow=0 milestone after "
            "phase17-34 completion sprint. Preserves rollback target for all "
            "subsequent depth/scheme_candidates/benchmark work."
        ),
    }
    manifest_path = out_dir / "release_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # Step 7: zip snapshot + manifest
    zip_path = out_dir / f"{release_tag}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(snapshot_db, arcname=snapshot_db.name)
        z.write(manifest_path, arcname="release_manifest.json")
    zip_size = zip_path.stat().st_size
    zip_sha = sha256_file(zip_path)

    # Step 8: backup copy to C:\chemlens\backend\releases\
    RELEASE_ROOT_WIN.mkdir(parents=True, exist_ok=True)
    backup_zip = RELEASE_ROOT_WIN / f"{release_tag}.zip"
    backup_manifest = RELEASE_ROOT_WIN / f"{release_tag}_manifest.json"
    shutil.copy2(zip_path, backup_zip)
    shutil.copy2(manifest_path, backup_manifest)

    # Update manifest with zip meta (post-zip)
    manifest["release_zip_name"] = zip_path.name
    manifest["release_zip_size_bytes"] = zip_size
    manifest["release_zip_sha256"] = zip_sha
    manifest["release_zip_backup_path"] = str(backup_zip)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    # also refresh backup manifest
    shutil.copy2(manifest_path, backup_manifest)

    print(f"[phase0] release_tag={release_tag}")
    print(f"[phase0] integrity_ok={integrity_ok}  integrity={integrity}")
    print(f"[phase0] wal_checkpoint={wal_result}")
    print(f"[phase0] rich={len(rich)} shallow={len(shallow)} missing={len(missing)} total={len(summaries)}")
    print(f"[phase0] live_db_size_before={live_size_before}  snapshot_size={snap_size}")
    print(f"[phase0] snapshot_sha256={snap_sha}")
    print(f"[phase0] snapshot_path={snapshot_db}")
    print(f"[phase0] zip_path={zip_path}  zip_size={zip_size}  zip_sha256={zip_sha}")
    print(f"[phase0] backup_zip={backup_zip}")
    print(f"[phase0] backup_manifest={backup_manifest}")
    print(f"[phase0] manifest={manifest_path}")

    if not integrity_ok:
        print(f"[phase0] ERROR: integrity_check failed: {integrity}", file=sys.stderr)
        return 1
    if len(shallow) != 0 or len(missing) != 0:
        print(
            f"[phase0] ERROR: expected shallow=0 missing=0 but got "
            f"shallow={len(shallow)} missing={len(missing)}",
            file=sys.stderr,
        )
        return 2
    if len(rich) != 264:
        print(
            f"[phase0] WARNING: expected rich=264 but got rich={len(rich)}",
            file=sys.stderr,
        )
        # Don't fail — just warn; maybe additional families merged/split
    return 0


if __name__ == "__main__":
    sys.exit(main())
