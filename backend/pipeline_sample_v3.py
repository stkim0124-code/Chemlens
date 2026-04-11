"""
pipeline_sample_v3.py
named_reactions*.zip 샘플 배치 파이프라인 v3

v2 대비 변경사항:
1. page_kind gate: toc/meta_explanatory → scheme_candidates=[], extract 0개, placeholder 금지
2. JSON schema normalizer: list 응답 자동 unwrap, 필수 key 보충
3. meta page SMILES 전면 NULL 강제
4. placeholder scheme 생성 금지 (빈 페이지 = DB row 0개)
5. manifest fieldnames 보강 (error 컬럼 포함)
6. page_images에 page_kind 칼럼 저장
7. 프롬프트 v2 적용 (page_kind gate 포함)
"""

import os, sys, json, base64, hashlib, zipfile, sqlite3, time, argparse, csv
from datetime import datetime
from pathlib import Path
import requests
from dotenv import load_dotenv

# ── 상수 ───────────────────────────────────────────────────────
PROMPT_VERSION = "nr_page_v2"
PIPELINE_VER   = "v3"
SAMPLE_N       = 15
SLEEP_SEC      = 2.0
MODEL          = "gemini-2.5-pro"
SOURCE_DOC     = "named reactions"

# page_kind: toc/meta_explanatory → scheme 추출 금지
META_PAGE_KINDS = {"toc", "meta_explanatory"}

# ── 프롬프트 로드 ───────────────────────────────────────────────
PROMPT_FILE = Path(__file__).parent / "prompt_named_reactions_v2.txt"

def load_prompt():
    if PROMPT_FILE.exists():
        text = PROMPT_FILE.read_text(encoding="utf-8")
        if "USER:" in text:
            return text.split("USER:", 1)[1].strip()
        return text.strip()
    raise FileNotFoundError("prompt_named_reactions_v2.txt not found")

SYSTEM_PROMPT = (
    "You are extracting structured chemical-reaction information from a named reactions textbook page. "
    "First classify the page as toc/meta_explanatory/reaction_content/unknown. "
    "For toc and meta_explanatory pages, return scheme_candidates as empty array []. "
    "Always return a single JSON object, never a JSON array. "
    "Be conservative. Do not invent chemistry. Do not convert R-groups to SMILES. "
    "Output valid JSON only. No markdown. No explanation."
)

# ── JSON 스키마 정규화 ──────────────────────────────────────────
REQUIRED_KEYS = {
    "page_kind": "unknown",
    "page_title": None,
    "reaction_family_name": None,
    "page_section_hints": [],
    "scheme_candidates": [],
}

def normalize_response(raw) -> dict:
    """
    Gemini 응답을 항상 dict로 정규화.
    - list 응답: 첫 원소 unwrap
    - None/invalid: 빈 기본값 반환
    - 필수 key 누락: 기본값 보충
    """
    # list → dict unwrap
    if isinstance(raw, list):
        if raw and isinstance(raw[0], dict):
            raw = raw[0]
        else:
            raw = {}

    if not isinstance(raw, dict):
        raw = {}

    # 필수 key 보충
    result = dict(REQUIRED_KEYS)
    result.update(raw)

    # page_kind 검증
    valid_kinds = {"toc", "meta_explanatory", "reaction_content", "unknown"}
    if result["page_kind"] not in valid_kinds:
        result["page_kind"] = "unknown"

    # scheme_candidates 검증
    if not isinstance(result["scheme_candidates"], list):
        result["scheme_candidates"] = []

    # page_section_hints 검증
    if not isinstance(result["page_section_hints"], list):
        result["page_section_hints"] = []

    # meta/toc 페이지: scheme_candidates 강제 초기화
    if result["page_kind"] in META_PAGE_KINDS:
        result["scheme_candidates"] = []
        result["reaction_family_name"] = None

    # 각 scheme 내부 정규화
    cleaned_schemes = []
    for sc in result["scheme_candidates"]:
        if not isinstance(sc, dict):
            continue
        sc = dict(sc)
        # extracts 검증
        if not isinstance(sc.get("extracts"), list):
            sc["extracts"] = []
        # 각 extract에서 SMILES 보수화
        cleaned_exts = []
        for ext in sc["extracts"]:
            if not isinstance(ext, dict):
                continue
            ext = dict(ext)
            # meta/toc가 아니어도 불확실한 SMILES는 NULL
            # smiles_confidence < 0.85 이면 NULL
            sc_val = float(ext.get("smiles_confidence") or 0.0)
            if sc_val < 0.85:
                ext["reactant_smiles"] = None
                ext["product_smiles"] = None
                ext["smiles_confidence"] = 0.0
            # 기본값 보충
            ext.setdefault("extraction_confidence", 0.5)
            cleaned_exts.append(ext)
        sc["extracts"] = cleaned_exts
        cleaned_schemes.append(sc)

    result["scheme_candidates"] = cleaned_schemes
    return result

# ── 유틸리티 ───────────────────────────────────────────────────
def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def img_to_b64(data: bytes) -> str:
    return base64.b64encode(data).decode()

def page_no(fname: str) -> int:
    return int(fname.split("_")[-1].replace(".jpg", ""))

def norm_name(s) -> str:
    if not s:
        return ""
    return str(s).lower().strip().replace("  ", " ")

def now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def find_zip(hint: str) -> Path:
    for c in [Path(hint), Path(hint.replace("_", " ")), Path(hint.replace(" ", "_"))]:
        if c.exists():
            return c
    for f in Path(".").iterdir():
        if f.suffix == ".zip" and "named" in f.name.lower() and "(" not in f.name:
            return f
    raise FileNotFoundError(f"ZIP not found: {hint}")

# ── Gemini API ─────────────────────────────────────────────────
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
            "response_mime_type": "application/json"
        }
    }
    r = requests.post(url, json=payload, timeout=120)
    data = r.json()
    if "error" in data:
        raise Exception(f"API {data['error'].get('code')}: {data['error'].get('message','')}")
    candidates = data.get("candidates", [])
    if not candidates:
        raise Exception(f"No candidates: {json.dumps(data)[:200]}")
    finish = candidates[0].get("finishReason", "")
    if finish not in ("STOP", "MAX_TOKENS", ""):
        raise Exception(f"finishReason: {finish}")
    text = candidates[0]["content"]["parts"][0]["text"].strip()
    # fence 방어
    if "```" in text:
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else parts[0]
        if text.startswith("json"):
            text = text[4:].strip()
    raw = json.loads(text)
    # 정규화 (list unwrap, schema fix)
    return normalize_response(raw)

# ── delete-and-rebuild ─────────────────────────────────────────
def delete_batch(conn: sqlite3.Connection, batch: str) -> int:
    pi_ids = [r[0] for r in conn.execute(
        "SELECT id FROM page_images WHERE ingest_batch=?", (batch,)
    ).fetchall()]
    if not pi_ids:
        return 0
    ph = ",".join("?" * len(pi_ids))
    sc_ids = [r[0] for r in conn.execute(
        f"SELECT id FROM scheme_candidates WHERE page_image_id IN ({ph})", pi_ids
    ).fetchall()]
    if sc_ids:
        sp = ",".join("?" * len(sc_ids))
        conn.execute(f"DELETE FROM reaction_extracts WHERE scheme_candidate_id IN ({sp})", sc_ids)
    conn.execute(f"DELETE FROM scheme_candidates WHERE page_image_id IN ({ph})", pi_ids)
    conn.execute("DELETE FROM page_images WHERE ingest_batch=?", (batch,))
    conn.commit()
    return len(pi_ids)

# ── 메인 ───────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip",     default=None)
    parser.add_argument("--db",      default="labint_round9_patched_v2.db")
    parser.add_argument("--batch",   default="sample_batch1")
    parser.add_argument("--n",       type=int, default=SAMPLE_N)
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args()

    load_dotenv(dotenv_path=".env")
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        print("ERROR: GEMINI_API_KEY not found")
        sys.exit(1)

    zip_path = find_zip(args.zip or "named reactions.zip")
    source_zip = zip_path.name
    user_prompt = load_prompt()

    print(f"=== pipeline_sample_v3 ({PIPELINE_VER}) ===")
    print(f"ZIP: {zip_path}  |  DB: {args.db}")
    print(f"Prompt: {PROMPT_VERSION}  |  Model: {MODEL}")
    print(f"Batch: {args.batch}  |  N: {args.n}")

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")

    if args.rebuild:
        n = delete_batch(conn, args.batch)
        print(f"[rebuild] Deleted {n} pages from batch '{args.batch}'")

    # 샘플 선택
    with zipfile.ZipFile(zip_path) as z:
        all_imgs = sorted(
            [f for f in z.namelist() if f.endswith(".jpg")],
            key=lambda x: page_no(x)
        )

    abbrev  = [f for f in all_imgs if page_no(f) < 46]
    reaction = [f for f in all_imgs if page_no(f) >= 46]
    sample  = abbrev[:3] + reaction[:args.n - 3]

    print(f"\n샘플 {len(sample)}장:")
    for f in sample:
        print(f"  p{page_no(f):3d}: {f}")
    print()

    ok = fail = skip = 0
    raw_dump = {}
    manifest_rows = []
    run_at = now_iso()

    # manifest fieldnames (error 포함)
    MF_FIELDS = [
        "page_no", "image_filename", "page_kind",
        "status", "scheme_count", "extract_count",
        "reaction_family_name", "error"
    ]

    with zipfile.ZipFile(zip_path) as z:
        for fname in sample:
            pno = page_no(fname)
            print(f"[p{pno:3d}] ", end="", flush=True)

            # idempotency (rebuild 아닌 경우)
            if not args.rebuild:
                ex = conn.execute(
                    "SELECT id FROM page_images "
                    "WHERE source_zip=? AND image_filename=? AND ingest_batch=?",
                    (source_zip, fname, args.batch)
                ).fetchone()
                if ex:
                    print("SKIP")
                    skip += 1
                    continue

            img_data = z.read(fname)
            sha = sha256_bytes(img_data)
            ts = now_iso()

            # 1. page_images 등록
            conn.execute("""
                INSERT OR REPLACE INTO page_images
                (source_zip, source_doc, page_no, image_filename,
                 sha256, file_size_bytes, ingest_batch, ingest_status,
                 created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (source_zip, SOURCE_DOC, pno, fname,
                  sha, len(img_data), args.batch, "registered", ts, ts))
            conn.commit()

            pi_id = conn.execute(
                "SELECT id FROM page_images WHERE source_zip=? AND image_filename=?",
                (source_zip, fname)
            ).fetchone()["id"]

            # 2. Gemini 호출
            try:
                result = gemini_call(img_data, api_key, user_prompt)
            except Exception as e:
                err = str(e)[:400]
                print(f"FAIL: {err}")
                conn.execute(
                    "UPDATE page_images SET ingest_status='error', "
                    "error_message=?, updated_at=? WHERE id=?",
                    (err, now_iso(), pi_id)
                )
                conn.commit()
                fail += 1
                manifest_rows.append({
                    "page_no": pno, "image_filename": fname,
                    "page_kind": "unknown", "status": "fail",
                    "scheme_count": 0, "extract_count": 0,
                    "reaction_family_name": None, "error": err
                })
                time.sleep(SLEEP_SEC)
                continue

            raw_dump[f"p{pno}"] = result
            page_kind = result.get("page_kind", "unknown")
            schemes   = result.get("scheme_candidates", [])
            n_schemes = len(schemes)
            family    = result.get("reaction_family_name")

            # page_kind 기록
            conn.execute(
                "UPDATE page_images SET page_kind=?, ingest_status='ocr_done', updated_at=? WHERE id=?",
                (page_kind, now_iso(), pi_id)
            )
            conn.commit()

            is_meta = (page_kind in META_PAGE_KINDS)
            print(
                f"OK | kind={page_kind} | "
                f"family={str(family or 'null')[:30]} | "
                f"schemes={n_schemes}"
            )

            # 3. toc/meta_explanatory: DB row 0개 (placeholder 금지)
            if is_meta or n_schemes == 0:
                conn.execute(
                    "UPDATE page_images SET ingest_status='parsed', updated_at=? WHERE id=?",
                    (now_iso(), pi_id)
                )
                conn.commit()
                ok += 1
                manifest_rows.append({
                    "page_no": pno, "image_filename": fname,
                    "page_kind": page_kind, "status": "ok",
                    "scheme_count": 0, "extract_count": 0,
                    "reaction_family_name": None, "error": None
                })
                time.sleep(SLEEP_SEC)
                continue

            # 4. reaction_content: scheme별 INSERT
            page_raw_json = json.dumps(result, ensure_ascii=False)
            total_extracts = 0

            for sch in schemes:
                sidx = sch.get("scheme_index", 1)
                ts2  = now_iso()

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
                    page_raw_json,
                    sch.get("confidence", 0.5),
                    MODEL, PROMPT_VERSION, ts2, ts2
                ))
                conn.commit()

                sc_id = conn.execute(
                    "SELECT id FROM scheme_candidates "
                    "WHERE page_image_id=? AND scheme_index=?",
                    (pi_id, sidx)
                ).fetchone()["id"]

                # 5. reaction_extracts
                extracts = sch.get("extracts", [])
                if not extracts:
                    # scheme는 있지만 extract 없는 경우: 1개 빈 row
                    extracts = [{}]

                for ext in extracts:
                    rfname = (ext.get("reaction_family_name")
                              or result.get("reaction_family_name"))
                    # SMILES는 이미 normalizer에서 처리됨
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
                        None,  # reactant_smiles: NULL (normalizer 처리 후)
                        None,  # product_smiles: NULL
                        0.0,   # smiles_confidence
                        ext.get("extraction_confidence", 0.5),
                        "raw", "hold",
                        MODEL, PROMPT_VERSION,
                        json.dumps(ext, ensure_ascii=False),
                        ts3, ts3
                    ))
                    total_extracts += 1
                conn.commit()

            conn.execute(
                "UPDATE page_images SET ingest_status='parsed', updated_at=? WHERE id=?",
                (now_iso(), pi_id)
            )
            conn.commit()

            ok += 1
            manifest_rows.append({
                "page_no": pno, "image_filename": fname,
                "page_kind": page_kind, "status": "ok",
                "scheme_count": n_schemes,
                "extract_count": total_extracts,
                "reaction_family_name": family,
                "error": None
            })
            time.sleep(SLEEP_SEC)

    # ── 결과 요약 ───────────────────────────────────────────────
    print(f"\n{'='*55}")
    print(f"완료: ok={ok}, fail={fail}, skip={skip}")
    print()

    print("── page_kind 분포 ──")
    kind_rows = conn.execute(
        "SELECT page_kind, ingest_status, COUNT(*) "
        "FROM page_images WHERE ingest_batch=? "
        "GROUP BY page_kind, ingest_status",
        (args.batch,)
    ).fetchall()
    for r in kind_rows:
        print(f"  {r[0] or 'null'} / {r[1]}: {r[2]}건")

    print()
    print("── DB 상태 ──")
    for tbl in ["page_images", "scheme_candidates", "reaction_extracts"]:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  {tbl}: {cnt}")
    rc = conn.execute("SELECT COUNT(*) FROM reaction_cards").fetchone()[0]
    print(f"  reaction_cards: {rc} (변경 없음 ✓)")

    print()
    print("── 추출 결과 요약 (reaction_content 페이지만) ──")
    rows = conn.execute("""
        SELECT pi.page_no, pi.page_kind,
               re.reaction_family_name, sc.section_type,
               sc.scheme_index, sc.confidence
        FROM reaction_extracts re
        JOIN scheme_candidates sc ON re.scheme_candidate_id = sc.id
        JOIN page_images pi ON sc.page_image_id = pi.id
        WHERE pi.ingest_batch=?
        ORDER BY pi.page_no, sc.scheme_index
    """, (args.batch,)).fetchall()

    for r in rows:
        print(f"  p{r['page_no']:3d} [{r['page_kind']}] "
              f"scheme#{r['scheme_index']} | "
              f"{str(r['reaction_family_name'] or 'N/A'):35s} | "
              f"{str(r['section_type'] or '?'):20s} | "
              f"conf={r['confidence'] or 0:.2f}")

    conn.close()

    # raw JSON dump
    dump_path = Path(args.db).parent / f"raw_json_dump_{args.batch}_v3.json"
    with open(dump_path, "w", encoding="utf-8") as f:
        json.dump({
            "run_at": run_at, "model": MODEL,
            "prompt_version": PROMPT_VERSION,
            "pipeline_version": PIPELINE_VER,
            "batch": args.batch, "source_zip": source_zip,
            "pages": raw_dump
        }, f, ensure_ascii=False, indent=2)
    print(f"\nRaw JSON: {dump_path}")

    # manifest CSV (error 칼럼 포함)
    mf_path = Path(args.db).parent / f"sample_manifest_{args.batch}_v3.csv"
    with open(mf_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MF_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(manifest_rows)
    print(f"Manifest: {mf_path}")

    print("\n[중요] reaction_cards는 변경하지 않았습니다.")
    print("[중요] promote는 GPT 검증 후 별도 단계에서만 수행합니다.")

if __name__ == "__main__":
    main()
