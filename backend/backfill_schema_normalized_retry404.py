import argparse, csv, json, sqlite3
from pathlib import Path
from datetime import datetime

import pipeline_named_reactions_v5 as mod

RESULT_FIELDS = [
    "source_zip", "page_no", "image_filename", "status",
    "schemes_before", "extracts_before", "schemes_after", "extracts_after", "error"
]


def now_iso():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def connect_db(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def load_page_filter(csv_path: str):
    if not csv_path:
        return None
    pages = set()
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            status = str(row.get("status", "")).strip().lower()
            if status and status not in {"ok", "success", "done"}:
                # if status column exists, only keep explicit ok rows for backfill target csvs
                continue
            p = row.get("page_no") or row.get("page")
            if p:
                pages.add(int(str(p).strip()))
    return pages


def get_page_row(conn, batch: str, source_zip: str, image_filename: str):
    return conn.execute(
        """
        SELECT id, page_no, ingest_status, page_kind
        FROM page_images
        WHERE ingest_batch=? AND source_zip=? AND image_filename=?
        ORDER BY id DESC LIMIT 1
        """,
        (batch, source_zip, image_filename),
    ).fetchone()


def count_scheme_extracts(conn, page_image_id: int):
    s = conn.execute("SELECT COUNT(*) AS c FROM scheme_candidates WHERE page_image_id=?", (page_image_id,)).fetchone()["c"]
    e = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM reaction_extracts re
        JOIN scheme_candidates sc ON re.scheme_candidate_id=sc.id
        WHERE sc.page_image_id=?
        """,
        (page_image_id,),
    ).fetchone()["c"]
    return int(s), int(e)


def clear_page_results(conn, page_image_id: int):
    sc_ids = [r[0] for r in conn.execute(
        "SELECT id FROM scheme_candidates WHERE page_image_id=?", (page_image_id,)
    ).fetchall()]
    if sc_ids:
        ph = ",".join("?" * len(sc_ids))
        conn.execute(f"DELETE FROM reaction_extracts WHERE scheme_candidate_id IN ({ph})", sc_ids)
    conn.execute("DELETE FROM scheme_candidates WHERE page_image_id=?", (page_image_id,))
    conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="labint_round9_v5.db")
    ap.add_argument("--batch", default="full_batch1")
    ap.add_argument("--raw-json", default="raw_json_dump_retry404_v5.json")
    ap.add_argument("--only-pages-csv", default=None)
    ap.add_argument("--zip-dir", default=None)
    ap.add_argument("--zip", default=None)
    ap.add_argument("--tag", default="schema_backfill_retry404")
    args = ap.parse_args()

    raw = json.loads(Path(args.raw_json).read_text(encoding="utf-8"))
    page_filter = load_page_filter(args.only_pages_csv)
    conn = connect_db(args.db)
    base = Path(args.db).parent
    zip_files = mod.find_zip_files(args.zip or "named reactions.zip", args.zip_dir) if (args.zip or args.zip_dir) else []
    zip_map = {p.name: p for p in zip_files}
    zip_cache = {}

    ok = 0
    fail = 0
    rows = []
    out_targets = []

    try:
        for tgt in raw.get("targets", []):
            source_zip = tgt.get("source_zip")
            page_no = int(tgt.get("page_no"))
            image_filename = tgt.get("image_filename")
            if page_filter is not None and page_no not in page_filter:
                continue
            row = get_page_row(conn, args.batch, source_zip, image_filename)
            if row is None:
                zip_path = zip_map.get(source_zip)
                if zip_path is None:
                    rows.append({
                        "source_zip": source_zip, "page_no": page_no, "image_filename": image_filename,
                        "status": "fail", "schemes_before": 0, "extracts_before": 0,
                        "schemes_after": 0, "extracts_after": 0,
                        "error": f"page_images row not found for batch={args.batch} and source ZIP not available"
                    })
                    fail += 1
                    continue
                if source_zip not in zip_cache:
                    zip_cache[source_zip] = __import__('zipfile').ZipFile(zip_path)
                z = zip_cache[source_zip]
                img_data = z.read(image_filename)
                sha = mod.sha256_bytes(img_data)
                pi_id = mod.insert_page_image(conn, source_zip, page_no, image_filename, sha, len(img_data), args.batch, mod.local_classify(page_no))
                conn.execute(
                    "UPDATE page_images SET page_kind=?, ingest_status='ocr_done', updated_at=? WHERE id=?",
                    (mod.local_classify(page_no), mod.now_iso(), pi_id),
                )
                conn.commit()
                row = get_page_row(conn, args.batch, source_zip, image_filename)

            try:
                schemes_before, extracts_before = count_scheme_extracts(conn, row["id"])
                normalized = mod.normalize_response(tgt.get("result") or {}, mod.local_classify(page_no))
                clear_page_results(conn, row["id"])
                total_extracts = mod.insert_schemes(conn, row["id"], normalized, source_zip, page_no)
                conn.execute(
                    "UPDATE page_images SET page_kind=?, ingest_status='parsed', updated_at=? WHERE id=?",
                    (normalized.get("page_kind") or row["page_kind"], mod.now_iso(), row["id"]),
                )
                conn.commit()
                schemes_after, extracts_after = count_scheme_extracts(conn, row["id"])
                rows.append({
                    "source_zip": source_zip, "page_no": page_no, "image_filename": image_filename,
                    "status": "ok", "schemes_before": schemes_before, "extracts_before": extracts_before,
                    "schemes_after": schemes_after, "extracts_after": extracts_after, "error": ""
                })
                out_targets.append({
                    "source_zip": source_zip,
                    "page_no": page_no,
                    "image_filename": image_filename,
                    "result": normalized,
                })
                print(f"[{source_zip} | p{page_no:3d}] backfill OK | schemes {schemes_before}->{schemes_after} | extracts {extracts_before}->{extracts_after}")
                ok += 1
            except Exception as e:
                conn.rollback()
                rows.append({
                    "source_zip": source_zip, "page_no": page_no, "image_filename": image_filename,
                    "status": "fail", "schemes_before": 0, "extracts_before": 0,
                    "schemes_after": 0, "extracts_after": 0,
                    "error": str(e)[:500]
                })
                print(f"[{source_zip} | p{page_no:3d}] backfill FAIL | {str(e)[:120]}")
                fail += 1
    finally:
        for z in zip_cache.values():
            try:
                z.close()
            except Exception:
                pass
        conn.close()

    raw_out = {
        "run_at": now_iso(),
        "batch": args.batch,
        "tag": args.tag,
        "targets": out_targets,
    }
    raw_path = base / f"raw_json_dump_{args.tag}_v5.json"
    mf_path = base / f"manifest_{args.tag}_v5.csv"
    raw_path.write_text(json.dumps(raw_out, ensure_ascii=False, indent=2), encoding="utf-8")
    with open(mf_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RESULT_FIELDS)
        w.writeheader()
        w.writerows(rows)

    print("=" * 60)
    print(f"schema backfill done: ok={ok}, fail={fail}")
    print(f"Raw JSON: {raw_path}")
    print(f"Manifest: {mf_path}")


if __name__ == "__main__":
    main()
