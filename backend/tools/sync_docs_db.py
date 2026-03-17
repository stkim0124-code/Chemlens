#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""sync_docs_db.py

Relink documents.file_path in labint_docs.db to actual PDFs present in backend/app/data/pdfs.

Design rules (project conventions)
- Store file_path as a *portable* relative path: "pdfs/<filename>.pdf".
- If documents.file_path currently contains an absolute path, we resolve by basename.
- We do NOT rely on UNIQUE(file_path). If you previously had UNIQUE, run
  tools/migrate_docs_remove_unique.py first.

Matching strategy
1) Exact filename match
2) Case-insensitive match
3) Whitespace-normalized match (collapse spaces)

Usage (Windows)
  python tools\sync_docs_db.py --db "app\data\labint_docs.db" --pdfs "app\data\pdfs" --apply

Dry-run
  python tools\sync_docs_db.py --db ... --pdfs ...

Output
- Writes sync_report.csv next to the DB.
"""

import argparse
import csv
import os
import re
import sqlite3
import sys
import ntpath
import unicodedata
from pathlib import Path


def nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)


def basename_any(p: str) -> str:
    if not p:
        return ""
    # Handles Windows paths even when running on non-Windows.
    b = ntpath.basename(p)
    if b == p:
        b = os.path.basename(p)
    return b


def norm_spaces(s: str) -> str:
    s = nfc(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def build_index(pdfs_dir: Path):
    pdfs = [p.name for p in pdfs_dir.iterdir() if p.is_file() and p.suffix.lower() == ".pdf"]
    idx = {}
    for name in pdfs:
        a = nfc(name)
        idx.setdefault(("exact", a), name)
        idx.setdefault(("lower", a.lower()), name)
        idx.setdefault(("sp", norm_spaces(a)), name)
        idx.setdefault(("sp_lower", norm_spaces(a).lower()), name)
    return pdfs, idx


def choose_match(basename: str, idx):
    b = nfc(basename)
    keys = [
        ("exact", b),
        ("lower", b.lower()),
        ("sp", norm_spaces(b)),
        ("sp_lower", norm_spaces(b).lower()),
    ]
    for k in keys:
        if k in idx:
            return idx[k]
    return None


def canonical_store_path(filename: str) -> str:
    # Always store as portable relative path
    return f"pdfs/{filename}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to labint_docs.db (SQLite)")
    ap.add_argument("--pdfs", required=True, help="Directory containing PDFs")
    ap.add_argument("--apply", action="store_true", help="Apply updates to DB (default: dry-run)")
    args = ap.parse_args()

    db_path = Path(args.db)
    pdfs_dir = Path(args.pdfs)
    if not db_path.exists():
        print(f"[ERR] DB not found: {db_path}")
        sys.exit(2)
    if not pdfs_dir.exists():
        print(f"[ERR] PDFs dir not found: {pdfs_dir}")
        sys.exit(2)

    pdf_list, idx = build_index(pdfs_dir)

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='documents'")
    if not cur.fetchone():
        print("[ERR] 'documents' table not found in DB.")
        sys.exit(3)

    cur.execute("PRAGMA table_info(documents)")
    cols = [r["name"] for r in cur.fetchall()]
    if "id" not in cols or "file_path" not in cols:
        print(f"[ERR] 'documents' must have id and file_path. Found columns: {cols}")
        sys.exit(3)

    rows = cur.execute("SELECT id, file_path FROM documents").fetchall()
    total = len(rows)

    report = []
    matched = 0
    changed = 0

    for r in rows:
        doc_id = r["id"]
        fp = r["file_path"] or ""

        # If already stored as pdfs/<name>[#id], normalize by stripping prefix/suffix to locate real filename
        fp_n = fp
        if fp_n.startswith("pdfs/"):
            fp_n = fp_n[len("pdfs/"):]
        if "#" in fp_n:
            fp_n = fp_n.split("#", 1)[0]

        base = basename_any(fp_n)
        m = choose_match(base, idx)

        status = "NO_MATCH"
        new_fp = fp
        if m:
            matched += 1
            new_fp = canonical_store_path(m)
            if fp != new_fp:
                status = "MATCH_UPDATE"
                changed += 1
                if args.apply:
                    cur.execute("UPDATE documents SET file_path=? WHERE id=?", (new_fp, doc_id))
            else:
                status = "MATCH"

        report.append({
            "id": doc_id,
            "old_file_path": fp,
            "old_basename": base,
            "new_file_path": new_fp,
            "status": status,
        })

    if args.apply:
        con.commit()
    con.close()

    out_csv = db_path.with_name("sync_report.csv")
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id", "old_file_path", "old_basename", "new_file_path", "status"])
        w.writeheader()
        w.writerows(report)

    print("---- sync_docs_db summary ----")
    print(f"PDF files in dir: {len(pdf_list)}")
    print(f"Documents in DB : {total}")
    if total:
        print(f"Matched         : {matched} ({matched/total*100:.1f}%)")
    else:
        print("Matched         : 0")
    print(f"Would update    : {changed}" + (" (APPLIED)" if args.apply else " (DRY-RUN)"))
    print(f"Report written  : {out_csv}")
    if not args.apply:
        print("\nRun again with --apply to write updates to DB.")


if __name__ == "__main__":
    main()
