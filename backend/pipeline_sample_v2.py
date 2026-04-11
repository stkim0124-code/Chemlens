"""
pipeline_sample_v2.py
named_reactions*.zip 샘플 배치 파이프라인 v2

GPT 검수 통과 버전:
- scheme_candidates: scheme별 1행 저장 (page-level 아님)
- 파일명 자동 인식 (named reactions.zip / named_reactions.zip 둘 다)
- GPT 합의 JSON 형식 사용
- delete-and-rebuild 재실행 전략
- provenance / prompt_version 완전 보존
- reaction_cards 절대 변경 없음
- 샘플 15장 only

실행 방법:
    python pipeline_sample_v2.py
    python pipeline_sample_v2.py --zip "named reactions.zip" --batch sample_batch1
    python pipeline_sample_v2.py --rebuild  # 동일 batch 재실행 (delete-and-rebuild)
"""

import os, sys, json, base64, hashlib, zipfile, sqlite3, time, argparse
from datetime import datetime
from pathlib import Path
import requests
from dotenv import load_dotenv

# ── 버전 상수 ───────────────────────────────────────────────────
PROMPT_VERSION  = "nr_page_v1"
PIPELINE_VER    = "v2"
SAMPLE_N        = 15
SLEEP_SEC       = 2.0
MODEL           = "gemini-2.5-pro"
SOURCE_DOC      = "named reactions"

# ── 프롬프트 로드 ────────────────────────────────────────────────
PROMPT_FILE = Path(__file__).parent / "prompt_named_reactions_v1.txt"

def load_prompt():
    if PROMPT_FILE.exists():
        text = PROMPT_FILE.read_text(encoding="utf-8")
        # USER: 이후 부분만 추출
        if "USER:" in text:
            return text.split("USER:", 1)[1].strip()
        return text.strip()
    # fallback
    return (
        "Extract the page into structured staging data. "
        "Return JSON only with page_title, reaction_family_name, "
        "page_section_hints, scheme_candidates (each with scheme_index, "
        "section_type, scheme_role, bbox_note, vision_summary, caption_text, "
        "nearby_text, confidence, extracts). "
        "No markdown, no explanation."
    )

SYSTEM_PROMPT = """You are extracting structured chemical-reaction information from a single textbook page image.
Be conservative. Do not invent chemistry. Do not convert generic R-groups to SMILES.
Distinguish: canonical_overview / mechanism_step / application_example / summary_box.
Output valid JSON only. No markdown fences. No explanations."""

# ── 유틸리티 ────────────────────────────────────────────────────
def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def img_to_b64(data: bytes) -> str:
    return base64.b64encode(data).decode()

def page_no(fname: str) -> int:
    return int(fname.split("_")[-1].replace(".jpg", ""))

def norm_name(s: str) -> str:
    if not s:
        return ""
    return s.lower().strip().replace("  ", " ")

def now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def find_zip(hint: str) -> Path:
    """파일명 mismatch 자동 해결: 여러 후보를 시도"""
    candidates = [
        Path(hint),
        Path(hint.replace("_", " ")),
        Path(hint.replace(" ", "_")),
    ]
    # 현재 폴더에서도 탐색
    for c in candidates:
        if c.exists():
            return c
    # 현재 폴더 전체 탐색
    for f in Path(".").iterdir():
        if f.suffix == ".zip" and "named" in f.name.lower() and "reaction" in f.name.lower():
            # (2)~(7) 아닌 첫번째 ZIP
            if "(" not in f.name:
                return f
    raise FileNotFoundError(f"ZIP not found. Tried: {[str(c) for c in candidates]}")

# ── Gemini API 호출 ──────────────────────────────────────────────
def gemini_call(img_data: bytes, api_key: str, user_prompt: str) -> dict:
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{MODEL}:generateContent?key={api_key}"
    )
    payload = {
        "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": [{
            "parts": [
                {"inline_data": {"mime_type": "image/jpeg", "data": img_to_b64(img_data)}},
                {"text": user_prompt}
            ]
        }],
        "generationConfig": {
            "temperature": 0.1,
            "response_mime_type": "application/json"   # JSON 강제
        }
    }
    r = requests.post(url, json=payload, timeout=120)
    data = r.json()

    if "error" in data:
        raise Exception(f"API error {data['error'].get('code')}: {data['error'].get('message','')}")

    candidates = data.get("candidates", [])
    if not candidates:
        raise Exception(f"No candidates in response: {json.dumps(data)[:300]}")

    finish = candidates[0].get("finishReason", "")
    if finish not in ("STOP", "MAX_TOKENS", ""):
        raise Exception(f"Unexpected finishReason: {finish}")

    text = candidates[0]["content"]["parts"][0]["text"].strip()

    # JSON 블록 방어 파싱
    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else parts[0]
        if text.startswith("json"):
            text = text[4:].strip()

    parsed = json.loads(text)
    return parsed

# ── DB 헬퍼 ─────────────────────────────────────────────────────
def delete_batch(conn: sqlite3.Connection, batch: str):
    """동일 batch의 staging 데이터를 완전 삭제 (delete-and-rebuild)"""
    page_ids = [r[0] for r in conn.execute(
        "SELECT id FROM page_images WHERE ingest_batch=?", (batch,)
    ).fetchall()]
    if not page_ids:
        return 0

    placeholders = ",".join("?" * len(page_ids))
    sc_ids = [r[0] for r in conn.execute(
        f"SELECT id FROM scheme_candidates WHERE page_image_id IN ({placeholders})",
        page_ids
    ).fetchall()]

    if sc_ids:
        sp = ",".join("?" * len(sc_ids))
        conn.execute(f"DELETE FROM reaction_extracts WHERE scheme_candidate_id IN ({sp})", sc_ids)

    conn.execute(
        f"DELETE FROM scheme_candidates WHERE page_image_id IN ({placeholders})", page_ids
    )
    conn.execute("DELETE FROM page_images WHERE ingest_batch=?", (batch,))
    conn.commit()
    return len(page_ids)

# ── 메인 파이프라인 ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip",     default=None, help="ZIP file path")
    parser.add_argument("--db",      default="labint_round9_patched.db")
    parser.add_argument("--batch",   default="sample_batch1")
    parser.add_argument("--n",       type=int, default=SAMPLE_N)
    parser.add_argument("--rebuild", action="store_true",
                        help="Delete existing batch data and reprocess (delete-and-rebuild)")
    args = parser.parse_args()

    load_dotenv(dotenv_path=".env")
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        print("ERROR: GEMINI_API_KEY not found in .env")
        sys.exit(1)

    # ZIP 자동 인식
    zip_hint = args.zip or "named reactions.zip"
    zip_path = find_zip(zip_hint)
    source_zip = zip_path.name
    print(f"ZIP: {zip_path}")

    user_prompt = load_prompt()
    print(f"Prompt version: {PROMPT_VERSION}")
    print(f"Model: {MODEL}")
    print(f"Batch: {args.batch}")
    print(f"Sample N: {args.n}")

    # DB 연결
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")

    # delete-and-rebuild
    if args.rebuild:
        deleted = delete_batch(conn, args.batch)
        print(f"[rebuild] Deleted {deleted} pages from batch '{args.batch}'")

    # 이미지 목록
    with zipfile.ZipFile(zip_path) as z:
        all_imgs = sorted(
            [f for f in z.namelist() if f.endswith(".jpg")],
            key=lambda x: page_no(x)
        )

    # 샘플 선택: 약어 테이블 3장 + 실제 반응 페이지 나머지
    abbrev = [f for f in all_imgs if page_no(f) < 46]
    reaction = [f for f in all_imgs if page_no(f) >= 46]
    sample = abbrev[:3] + reaction[:args.n - 3]

    print(f"\n선택된 샘플 {len(sample)}장:")
    for f in sample:
        print(f"  p{page_no(f):3d}: {f}")
    print()

    ok = fail = skip = 0
    raw_dump = {}
    manifest_rows = []
    run_at = now_iso()

    with zipfile.ZipFile(zip_path) as z:
        for fname in sample:
            pno = page_no(fname)
            print(f"[p{pno:3d}] ", end="", flush=True)

            # idempotency check (rebuild 하지 않은 경우)
            if not args.rebuild:
                existing = conn.execute(
                    "SELECT id FROM page_images "
                    "WHERE source_zip=? AND image_filename=? AND ingest_batch=?",
                    (source_zip, fname, args.batch)
                ).fetchone()
                if existing:
                    print("SKIP (already processed in this batch)")
                    skip += 1
                    continue

            img_data = z.read(fname)
            sha = sha256_bytes(img_data)
            fsize = len(img_data)

            # ── 1. page_images 등록 ─────────────────────────────
            ts = now_iso()
            conn.execute("""
                INSERT OR REPLACE INTO page_images
                (source_zip, source_doc, page_no, image_filename,
                 sha256, file_size_bytes, ingest_batch, ingest_status,
                 created_at, updated_at)
                VALUES (?,?,?,?,?,?,'""" + args.batch + """','registered',?,?)
            """, (source_zip, SOURCE_DOC, pno, fname, sha, fsize, ts, ts))
            conn.commit()

            pi_id = conn.execute(
                "SELECT id FROM page_images WHERE source_zip=? AND image_filename=?",
                (source_zip, fname)
            ).fetchone()["id"]

            # ── 2. Gemini Vision 호출 ───────────────────────────
            try:
                result = gemini_call(img_data, api_key, user_prompt)
                raw_dump[f"p{pno}"] = result
                page_type_hint = "reaction" if result.get("reaction_family_name") else "non-reaction"
                n_schemes = len(result.get("scheme_candidates", []))
                print(f"OK | {page_type_hint} | {n_schemes} schemes | "
                      f"conf≈{result.get('scheme_candidates',[{}])[0].get('confidence','?') if n_schemes else 'N/A'}")

                # page_images 상태 업데이트
                conn.execute(
                    "UPDATE page_images SET ingest_status='ocr_done', updated_at=? WHERE id=?",
                    (now_iso(), pi_id)
                )
                conn.commit()

            except Exception as e:
                err = str(e)[:400]
                print(f"FAIL: {err}")
                conn.execute(
                    "UPDATE page_images SET ingest_status='error', error_message=?, updated_at=? WHERE id=?",
                    (err, now_iso(), pi_id)
                )
                conn.commit()
                fail += 1
                manifest_rows.append({
                    "page_no": pno, "filename": fname, "status": "fail", "error": err
                })
                time.sleep(SLEEP_SEC)
                continue

            # ── 3. scheme_candidates: scheme별 1행 ─────────────
            schemes = result.get("scheme_candidates", [])

            # 비-반응 페이지 (표지/목차 등): 빈 scheme 1개 생성하여 페이지 추적
            if not schemes:
                schemes = [{
                    "scheme_index": 1,
                    "section_type": "unknown",
                    "scheme_role": "unknown",
                    "bbox_note": None,
                    "vision_summary": f"Non-reaction page: {result.get('page_title','unknown')}",
                    "caption_text": None,
                    "nearby_text": None,
                    "confidence": 0.5,
                    "extracts": []
                }]

            page_raw_json = json.dumps(result, ensure_ascii=False)

            for sch in schemes:
                sidx = sch.get("scheme_index", 1)
                ts2 = now_iso()

                # scheme_candidates INSERT (scheme별 1행)
                conn.execute("""
                    INSERT OR REPLACE INTO scheme_candidates
                    (page_image_id, scheme_index, section_type, scheme_role,
                     nearby_text, caption_text, vision_summary, vision_raw_json,
                     confidence, review_status,
                     detector_model, detector_prompt_version,
                     created_at, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,'unreviewed',?,?,?,?)
                """, (
                    pi_id, sidx,
                    sch.get("section_type"),
                    sch.get("scheme_role"),
                    sch.get("nearby_text"),
                    sch.get("caption_text"),
                    sch.get("vision_summary"),
                    page_raw_json,      # 페이지 전체 raw JSON을 보존
                    sch.get("confidence", 0.5),
                    MODEL, PROMPT_VERSION,
                    ts2, ts2
                ))
                conn.commit()

                sc_id = conn.execute(
                    "SELECT id FROM scheme_candidates "
                    "WHERE page_image_id=? AND scheme_index=?",
                    (pi_id, sidx)
                ).fetchone()["id"]

                # ── 4. reaction_extracts: extract별 INSERT ──────
                extracts = sch.get("extracts", [])
                if not extracts:
                    extracts = [{}]   # 빈 extract 1개 (정보 없음)

                for ext in extracts:
                    rfname = ext.get("reaction_family_name") or result.get("reaction_family_name")
                    ts3 = now_iso()
                    conn.execute("""
                        INSERT INTO reaction_extracts
                        (scheme_candidate_id,
                         reaction_family_name, reaction_family_name_norm,
                         extract_kind,
                         transformation_text,
                         reactants_text, products_text, intermediates_text,
                         reagents_text, catalysts_text, solvents_text,
                         temperature_text, time_text, yield_text,
                         workup_text, conditions_text, notes_text,
                         reactant_smiles, product_smiles,
                         smiles_confidence, extraction_confidence,
                         parse_status, promote_decision,
                         extractor_model, extractor_prompt_version,
                         extraction_raw_json,
                         created_at, updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        sc_id,
                        rfname, norm_name(rfname),
                        ext.get("extract_kind", "unknown"),
                        ext.get("transformation_text"),
                        ext.get("reactants_text"),
                        ext.get("products_text"),
                        ext.get("intermediates_text"),
                        ext.get("reagents_text"),
                        ext.get("catalysts_text"),
                        ext.get("solvents_text"),
                        ext.get("temperature_text"),
                        ext.get("time_text"),
                        ext.get("yield_text"),
                        ext.get("workup_text"),
                        ext.get("conditions_text"),
                        ext.get("notes_text"),
                        None,   # reactant_smiles: 반드시 NULL (generic R-group 보호)
                        None,   # product_smiles: 반드시 NULL
                        ext.get("smiles_confidence", 0.0),
                        ext.get("extraction_confidence", 0.5),
                        "raw", "hold",
                        MODEL, PROMPT_VERSION,
                        json.dumps(ext, ensure_ascii=False),
                        ts3, ts3
                    ))
                conn.commit()

            # page_images 최종 상태
            conn.execute(
                "UPDATE page_images SET ingest_status='parsed', updated_at=? WHERE id=?",
                (now_iso(), pi_id)
            )
            conn.commit()

            ok += 1
            manifest_rows.append({
                "page_no": pno, "filename": fname,
                "status": "ok",
                "reaction_family": result.get("reaction_family_name"),
                "n_schemes": len(result.get("scheme_candidates", [])),
                "page_title": result.get("page_title")
            })
            time.sleep(SLEEP_SEC)

    # ── 최종 집계 ────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"완료: ok={ok}, fail={fail}, skip={skip}")

    print("\n── DB 상태 ──")
    for tbl in ["page_images", "scheme_candidates", "reaction_extracts"]:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  {tbl}: {cnt}")

    rc = conn.execute("SELECT COUNT(*) FROM reaction_cards").fetchone()[0]
    print(f"  reaction_cards: {rc} (변경 없음 ✓)")

    print("\n── 추출 결과 요약 ──")
    rows = conn.execute("""
        SELECT pi.page_no, re.reaction_family_name, sc.section_type,
               sc.scheme_role, sc.scheme_index, sc.confidence
        FROM reaction_extracts re
        JOIN scheme_candidates sc ON re.scheme_candidate_id = sc.id
        JOIN page_images pi ON sc.page_image_id = pi.id
        ORDER BY pi.page_no, sc.scheme_index
    """).fetchall()

    for r in rows:
        print(f"  p{r['page_no']:3d} | scheme#{r['scheme_index']} "
              f"| {str(r['reaction_family_name'] or 'N/A'):35s} "
              f"| {str(r['section_type'] or '?'):20s} "
              f"| conf={r['confidence'] or 0:.2f}")

    conn.close()

    # raw JSON dump 저장
    dump_path = Path(args.db).parent / f"raw_json_dump_{args.batch}.json"
    with open(dump_path, "w", encoding="utf-8") as f:
        json.dump({
            "run_at": run_at, "model": MODEL,
            "prompt_version": PROMPT_VERSION,
            "batch": args.batch,
            "source_zip": source_zip,
            "pages": raw_dump
        }, f, ensure_ascii=False, indent=2)
    print(f"\nRaw JSON dump: {dump_path}")

    # manifest CSV
    import csv
    mf_path = Path(args.db).parent / f"sample_manifest_{args.batch}.csv"
    with open(mf_path, "w", newline="", encoding="utf-8") as f:
        if manifest_rows:
            w = csv.DictWriter(f, fieldnames=manifest_rows[0].keys())
            w.writeheader()
            w.writerows(manifest_rows)
    print(f"Manifest: {mf_path}")
    print("\n[중요] reaction_cards는 변경하지 않았습니다.")
    print("[중요] promote는 GPT 검증 후 별도 단계에서만 수행합니다.")

if __name__ == "__main__":
    main()
