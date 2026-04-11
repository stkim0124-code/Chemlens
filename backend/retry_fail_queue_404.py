import os, sys, csv, json, time, sqlite3, argparse, zipfile
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

import pipeline_named_reactions_v5 as mod

RESULT_FIELDS = [
    "source_zip", "page_no", "image_filename", "status", "schemes", "extracts", "error"
]


def now_iso():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def connect_db(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def read_rows_from_csv(csv_path: str, default_zip: str = None):
    rows = []
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            status = str(row.get("status", "")).strip().lower()
            if status and status not in {"fail", "error"}:
                continue
            pno = row.get("page_no") or row.get("page")
            fname = row.get("image_filename") or row.get("filename")
            if not pno or not fname:
                continue
            rows.append({
                "source_zip": row.get("source_zip") or default_zip or "named reactions.zip",
                "page_no": int(str(pno).strip()),
                "image_filename": str(fname).strip(),
                "page_kind": row.get("page_kind") or mod.local_classify(int(str(pno).strip())),
                "last_error_message": row.get("error") or row.get("last_error_message") or "",
            })
    return rows


def select_retry_rows(conn, batch: str, limit: int = 0):
    q = """
    SELECT source_zip, page_no, image_filename, page_kind, failure_type,
           attempts, last_error_message, batch, created_at
    FROM fail_queue
    WHERE batch = ?
      AND (
            failure_type = 'api_404_model'
         OR (failure_type = 'unknown' AND last_error_message LIKE '%API 404%')
         OR (failure_type = 'unknown' AND last_error_message LIKE '%models/%')
         OR last_error_message LIKE '%not found%'
      )
    ORDER BY page_no
    """
    rows = conn.execute(q, (batch,)).fetchall()
    if limit and limit > 0:
        rows = rows[:limit]
    return [dict(r) for r in rows]


def existing_parsed_page_id(conn, batch: str, source_zip: str, image_filename: str):
    row = conn.execute(
        """
        SELECT id FROM page_images
        WHERE ingest_batch=? AND source_zip=? AND image_filename=? AND ingest_status='parsed'
        ORDER BY id DESC LIMIT 1
        """,
        (batch, source_zip, image_filename),
    ).fetchone()
    return row["id"] if row else None


def delete_page_batch(conn, batch: str, source_zip: str, image_filename: str):
    rows = conn.execute(
        "SELECT id FROM page_images WHERE source_zip=? AND image_filename=? AND ingest_batch=?",
        (source_zip, image_filename, batch),
    ).fetchall()
    for row in rows:
        pi_id = row[0]
        sc_ids = [r[0] for r in conn.execute(
            "SELECT id FROM scheme_candidates WHERE page_image_id=?", (pi_id,)
        ).fetchall()]
        if sc_ids:
            ph = ",".join("?" * len(sc_ids))
            conn.execute(f"DELETE FROM reaction_extracts WHERE scheme_candidate_id IN ({ph})", sc_ids)
        conn.execute("DELETE FROM scheme_candidates WHERE page_image_id=?", (pi_id,))
        conn.execute("DELETE FROM page_images WHERE id=?", (pi_id,))
    conn.execute(
        "DELETE FROM fail_queue WHERE batch=? AND source_zip=? AND image_filename=?",
        (batch, source_zip, image_filename),
    )
    conn.commit()


def call_primary_only(img_data: bytes, api_key: str, user_prompt: str, local_kind: str = None):
    attempts = 0
    last_err = None
    strategies = [
        (mod.MODEL_PRIMARY, user_prompt, 0),
        (mod.MODEL_PRIMARY, mod.FALLBACK_PROMPT, mod.SLEEP_RECITATION),
        (mod.MODEL_PRIMARY, user_prompt, mod.SLEEP_TIMEOUT_1),
    ]
    for model, prompt, sleep_before in strategies:
        if sleep_before > 0:
            time.sleep(sleep_before)
        attempts += 1
        try:
            raw = mod._call_once(img_data, api_key, prompt, model)
            result = mod.normalize_response(raw, local_kind)
            return result, attempts, None
        except mod.Api503Error as e:
            last_err = e
            wait = [mod.SLEEP_503_1, mod.SLEEP_503_2, mod.SLEEP_503_3][min(attempts - 1, 2)]
            print(f"(primary-only 503 retry {attempts}, wait {wait}s)", end=" ", flush=True)
            time.sleep(wait)
            continue
        except mod.TimeoutError_ as e:
            last_err = e
            wait = mod.SLEEP_TIMEOUT_1 if attempts == 1 else mod.SLEEP_TIMEOUT_2
            print(f"(primary-only timeout retry {attempts}, wait {wait}s)", end=" ", flush=True)
            time.sleep(wait)
            continue
        except mod.RecitationError as e:
            last_err = e
            print(f"(primary-only recitation retry {attempts})", end=" ", flush=True)
            continue
        except Exception as e:
            last_err = e
            continue
    raise Exception(f"primary_only_retry_failed after {attempts} attempts: {last_err}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="labint_round9_v5.db")
    ap.add_argument("--batch", default="full_batch1")
    ap.add_argument("--zip-dir", default=None)
    ap.add_argument("--zip", default=None)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--fail-csv", default=None)
    ap.add_argument("--tag", default="retry404")
    ap.add_argument("--skip-existing", action="store_true")
    args = ap.parse_args()

    load_dotenv(dotenv_path=".env")
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        print("ERROR: GEMINI_API_KEY not found")
        sys.exit(1)

    conn = connect_db(args.db)
    if args.fail_csv:
        rows = read_rows_from_csv(args.fail_csv, Path(args.zip).name if args.zip else None)
        selection_label = f"CSV:{Path(args.fail_csv).name}"
    else:
        rows = select_retry_rows(conn, args.batch, args.limit)
        selection_label = "fail_queue DB (404 filter)"
    if args.limit and args.limit > 0:
        rows = rows[:args.limit]
    if not rows:
        print("No retry targets found.")
        conn.close()
        return

    zip_files = mod.find_zip_files(args.zip or "named reactions.zip", args.zip_dir)
    zip_map = {p.name: p for p in zip_files}
    user_prompt = mod.load_prompt()

    print("=== retry_fail_queue_404 ===")
    print(f"DB={args.db} | batch={args.batch}")
    print(f"selection={selection_label}")
    print(f"targets={len(rows)} | primary-only retry (fallback model disabled)")

    zip_cache = {}
    results = []
    raw_dump = {"run_at": now_iso(), "batch": args.batch, "tag": args.tag, "targets": []}
    ok = fail = skip_existing = 0

    try:
        for r in rows:
            source_zip = r["source_zip"]
            pno = int(r["page_no"])
            fname = r["image_filename"]
            local_kind = mod.local_classify(pno)
            if args.skip_existing and existing_parsed_page_id(conn, args.batch, source_zip, fname):
                print(f"[{source_zip} | p{pno:3d}] skip_existing")
                results.append({
                    "source_zip": source_zip, "page_no": pno, "image_filename": fname,
                    "status": "skip_existing", "schemes": 0, "extracts": 0, "error": None
                })
                skip_existing += 1
                continue
            print(f"[{source_zip} | p{pno:3d}] retry404 ", end="", flush=True)

            zip_path = zip_map.get(source_zip)
            if zip_path is None:
                err = f"source_zip not found among discovered ZIPs: {source_zip}"
                print(f"FAIL: {err}")
                results.append({
                    "source_zip": source_zip, "page_no": pno, "image_filename": fname,
                    "status": "fail", "schemes": 0, "extracts": 0, "error": err
                })
                fail += 1
                continue

            if source_zip not in zip_cache:
                zip_cache[source_zip] = zipfile.ZipFile(zip_path)
            z = zip_cache[source_zip]
            img_data = z.read(fname)
            sha = mod.sha256_bytes(img_data)

            try:
                result, attempts, _ = call_primary_only(img_data, api_key, user_prompt, local_kind)
            except Exception as e:
                err = str(e)[:500]
                print(f"FAIL: {err[:120]}")
                mod.insert_fail_queue(conn, source_zip, pno, fname, local_kind, "retry404_failed", 3, err, args.batch)
                results.append({
                    "source_zip": source_zip, "page_no": pno, "image_filename": fname,
                    "status": "fail", "schemes": 0, "extracts": 0, "error": err
                })
                fail += 1
                continue

            delete_page_batch(conn, args.batch, source_zip, fname)
            pi_id = mod.insert_page_image(conn, source_zip, pno, fname, sha, len(img_data), args.batch, result.get("page_kind", local_kind))
            conn.execute(
                "UPDATE page_images SET page_kind=?, ingest_status='ocr_done', updated_at=? WHERE id=?",
                (result.get("page_kind", local_kind), mod.now_iso(), pi_id),
            )
            conn.commit()
            total_extracts = 0
            if result.get("scheme_candidates"):
                total_extracts = mod.insert_schemes(conn, pi_id, result, source_zip, pno)
            conn.execute(
                "UPDATE page_images SET ingest_status='parsed', updated_at=? WHERE id=?",
                (mod.now_iso(), pi_id),
            )
            conn.commit()

            raw_dump["targets"].append({
                "source_zip": source_zip,
                "page_no": pno,
                "image_filename": fname,
                "result": result,
            })
            scount = len(result.get("scheme_candidates") or [])
            print(f"OK | kind={result.get('page_kind')} | family={result.get('reaction_family_name')} | schemes={scount} | extracts={total_extracts}")
            results.append({
                "source_zip": source_zip, "page_no": pno, "image_filename": fname,
                "status": "ok", "schemes": scount, "extracts": total_extracts, "error": None
            })
            ok += 1
            time.sleep(mod.SLEEP_NORMAL)
    finally:
        for z in zip_cache.values():
            try:
                z.close()
            except Exception:
                pass
        conn.close()

    base = Path(args.db).parent
    raw_path = base / f"raw_json_dump_{args.tag}_v5.json"
    mf_path = base / f"manifest_{args.tag}_v5.csv"
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(raw_dump, f, ensure_ascii=False, indent=2)
    with open(mf_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RESULT_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(results)

    print("=" * 60)
    print(f"retry404 done: ok={ok}, fail={fail}, skip_existing={skip_existing}")
    print(f"Raw JSON: {raw_path}")
    print(f"Manifest: {mf_path}")


if __name__ == "__main__":
    main()
