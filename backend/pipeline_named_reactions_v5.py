"""
pipeline_named_reactions_v5.py
named_reactions*.zip 운영형 파이프라인 v5

v4 대비 변경사항:
1. 로컬 page_kind 선분류 → Gemini는 reaction_content에만 호출
2. retry/backoff: 503/timeout/recitation별 구분 처리
3. fallback model: gemini-2.5-flash
4. fail_queue 테이블 + CSV 산출
5. sample/full 모드 분리 (--mode sample|full)
6. SMILES confidence 임계값 0.90으로 강화
7. staging only, reaction_cards 절대 변경 없음

실행:
    python pipeline_named_reactions_v5.py --mode sample --rebuild
    python pipeline_named_reactions_v5.py --mode full --rebuild
"""

import os, sys, json, base64, hashlib, zipfile, sqlite3, time, argparse, csv, re
from datetime import datetime
from pathlib import Path
import requests
from dotenv import load_dotenv
from typing import List, Tuple, Any, Dict

# ── 상수 ───────────────────────────────────────────────────────
PROMPT_VERSION   = "nr_page_v3"
PIPELINE_VER     = "v5"
SLEEP_NORMAL     = 1.5   # 정상 처리 후 대기
SLEEP_503_1      = 5     # 503 1차 재시도 전 대기
SLEEP_503_2      = 15    # 503 2차 재시도 전 대기
SLEEP_503_3      = 45    # 503 3차 재시도 전 대기
SLEEP_TIMEOUT_1  = 10    # timeout 1차 재시도 전 대기
SLEEP_TIMEOUT_2  = 30    # timeout 2차 재시도 전 대기
SLEEP_RECITATION = 5     # recitation 재시도 전 대기
MODEL_PRIMARY    = "gemini-2.5-pro"
MODEL_FALLBACK   = "gemini-2.5-flash-preview-04-17"  # fallback
SOURCE_DOC       = "named reactions"
SMILES_MIN_CONF  = 0.90  # SMILES 허용 최소 confidence

# ── 로컬 page_kind 선분류 규칙 ──────────────────────────────────
# named reactions.zip (p14~p145) 기준
# v3 샘플 결과 기반으로 확인된 패턴:
#   p14~p16: meta_explanatory (색상설명/서문)
#   p17~p53: toc (약어테이블 + 목차/구분 페이지)
#   p54~ : reaction_content

LOCAL_TOC_RANGE        = (17, 53)   # 약어테이블 + TOC
LOCAL_META_RANGE       = (14, 16)   # 서문/색상설명
LOCAL_REACTION_START   = 54         # 실제 반응 페이지 시작

def local_classify(pno: int) -> str:
    """
    페이지 번호만으로 로컬 선분류.
    Gemini 호출 없이 즉시 분류 가능한 페이지들을 처리.
    Returns: 'toc' | 'meta_explanatory' | 'reaction_content' | 'unknown'
    """
    lo, hi = LOCAL_META_RANGE
    if lo <= pno <= hi:
        return "meta_explanatory"
    lo, hi = LOCAL_TOC_RANGE
    if lo <= pno <= hi:
        return "toc"
    if pno >= LOCAL_REACTION_START:
        return "reaction_content"
    return "unknown"

META_PAGE_KINDS = {"toc", "meta_explanatory"}

# ── 프롬프트 ───────────────────────────────────────────────────
PROMPT_FILE = Path(__file__).parent / "prompt_named_reactions_v3.txt"

def load_prompt() -> str:
    if PROMPT_FILE.exists():
        text = PROMPT_FILE.read_text(encoding="utf-8")
        if "USER:" in text:
            return text.split("USER:", 1)[1].strip()
        return text.strip()
    raise FileNotFoundError("prompt_named_reactions_v3.txt not found")

SYSTEM_PROMPT = (
    "You are extracting structured chemical-reaction information from a named reactions textbook page. "
    "First classify page_kind as toc/meta_explanatory/reaction_content/unknown. "
    "For toc and meta_explanatory pages, return scheme_candidates as []. "
    "Always return a single JSON object, never a JSON array. "
    "Use Title Case for reaction_family_name. "
    "Be conservative. Do not invent chemistry. Do not convert R-groups to SMILES. "
    "Output valid JSON only. No markdown. No explanation."
)

FALLBACK_PROMPT = """Analyze this chemistry textbook page.
Classify page_kind as: toc, meta_explanatory, reaction_content, or unknown.
For toc/meta_explanatory: scheme_candidates=[].

Return ONLY this JSON object:
{
  "page_kind": "reaction_content",
  "page_title": null,
  "reaction_family_name": null,
  "page_section_hints": [],
  "scheme_candidates": []
}
Output JSON only. No markdown."""

# ── Title Case 정규화 ───────────────────────────────────────────
_LOWER_WORDS = {"a","an","the","and","or","of","in","on","at","to","for",
                "via","with","by","from","as","per","vs","vs."}

def to_title_case(s: str) -> str:
    if not s:
        return s
    words = s.strip().split()
    result = []
    for i, w in enumerate(words):
        if '-' in w:
            result.append('-'.join(p.capitalize() for p in w.split('-')))
        else:
            w_lower = w.lower()
            result.append(w_lower.capitalize() if (i == 0 or w_lower not in _LOWER_WORDS)
                          else w_lower)
    return " ".join(result)

def canonical_family(raw) -> str:
    if not raw:
        return raw
    return to_title_case(str(raw))

# ── JSON 정규화 ────────────────────────────────────────────────

REQUIRED_KEYS = {
    "page_kind": "unknown",
    "page_title": None,
    "reaction_family_name": None,
    "page_section_hints": [],
    "scheme_candidates": [],
}


def _as_list(v):
    if v is None:
        return []
    return v if isinstance(v, list) else [v]


def _looks_like_smiles(s: str) -> bool:
    if not s or not isinstance(s, str):
        return False
    s = s.strip()
    if not s:
        return False
    # overly permissive is OK for text display, but avoid obvious prose.
    if any(tok in s.lower() for tok in [" yield", "step ", "conditions", "reaction", "mechanism", "synthe", "rearrangement"]):
        return False
    return bool(re.search(r"[=#\/\[\]\(\)@+\-]", s) or re.fullmatch(r"[A-Za-z0-9]+", s))


def _entity_text(obj: Any) -> str:
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj.strip()
    if isinstance(obj, (int, float)):
        return str(obj)
    if isinstance(obj, dict):
        parts = []
        for key in [
            "name", "label", "reagent_name", "product_name", "reaction_name",
            "description", "comment", "conditions", "yield", "diastereoselectivity",
            "stereochemistry", "value", "unit", "smiles", "structure", "role"
        ]:
            val = obj.get(key)
            if val is None or val == "":
                continue
            sval = str(val).strip()
            if sval and sval not in parts:
                parts.append(sval)
        return " | ".join(parts)
    if isinstance(obj, list):
        vals = [_entity_text(x) for x in obj]
        vals = [v for v in vals if v]
        return "; ".join(vals)
    return str(obj).strip()


def _join_texts(items: Any) -> str:
    vals = []
    for x in _as_list(items):
        s = _entity_text(x)
        if s:
            vals.append(s)
    # dedupe while preserving order
    out = []
    seen = set()
    for s in vals:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return "; ".join(out)


def _split_reagents(items: Any):
    reagents, catalysts, solvents, other = [], [], [], []
    for x in _as_list(items):
        if isinstance(x, dict):
            role = str(x.get("role") or x.get("reagent_role") or "").lower()
            txt = _entity_text(x)
            if not txt:
                continue
            if "solvent" in role:
                solvents.append(txt)
            elif "catalyst" in role:
                catalysts.append(txt)
            elif role in {"condition", "conditions"}:
                other.append(txt)
            else:
                reagents.append(txt)
        else:
            txt = _entity_text(x)
            if txt:
                reagents.append(txt)
    def _dedupe(seq):
        out=[]; seen=set()
        for s in seq:
            if s not in seen:
                seen.add(s); out.append(s)
        return "; ".join(out)
    return _dedupe(reagents), _dedupe(catalysts), _dedupe(solvents), _dedupe(other)


def _extract_conditions_from_details(details: Any):
    if not isinstance(details, dict):
        return None, None, None, None
    temp = details.get("temperature")
    timev = details.get("time")
    cond_bits = []
    notes_bits = []
    for k, v in details.items():
        if v in (None, ""):
            continue
        if k in {"temperature", "time"}:
            continue
        if k in {"conditions", "condition", "mechanism"}:
            cond_bits.append(f"{k}: {v}")
        elif k in {"notes", "note"}:
            notes_bits.append(str(v))
        else:
            notes_bits.append(f"{k}: {v}")
    cond = "; ".join(cond_bits) or None
    notes = "; ".join(notes_bits) or None
    return str(temp) if temp not in (None, "") else None, str(timev) if timev not in (None, "") else None, cond, notes


def _infer_section_type(sch: dict, page_hints: list) -> str:
    joined = " ".join(str(x) for x in page_hints if x).lower()
    blob = " ".join(
        str(sch.get(k) or "") for k in [
            "scheme_name", "reaction_name", "reaction_type", "description",
            "reaction_stage", "scheme_text", "scheme_id", "reaction_id"
        ]
    ).lower()
    if sch.get("is_mechanism") or sch.get("is_mechanistic_scheme") or sch.get("is_catalytic_cycle"):
        return "mechanism"
    if "synthetic application" in blob or "application" in blob or sch.get("final_product_of_synthesis"):
        return "synthetic_application"
    if "mechanism" in blob or "catalytic cycle" in blob:
        return "mechanism"
    if sch.get("is_overall_reaction") or sch.get("is_overall_scheme") or sch.get("is_overall_reaction_of_page"):
        return "overview"
    if "overall reaction" in blob or "overall scheme" in blob:
        return "overview"
    if len(page_hints) == 1 and ("synthetic application" in joined or "synthetic applications" in joined):
        return "synthetic_application"
    return "overview"


def _infer_scheme_role(section_type: str) -> str:
    if section_type == "mechanism":
        return "mechanism_step"
    if section_type == "synthetic_application":
        return "application_example"
    return "canonical_overview"


def _infer_extract_kind(section_type: str) -> str:
    if section_type == "mechanism":
        return "mechanism_step"
    if section_type == "synthetic_application":
        return "application_example"
    return "canonical_overview"


def _coalesce(*vals):
    for v in vals:
        if v not in (None, "", [], {}):
            return v
    return None


def _build_auto_extract(sch: dict, page_family: str, page_hints: list) -> dict:
    section_type = sch.get("section_type") or _infer_section_type(sch, page_hints)
    extract_kind = _infer_extract_kind(section_type)
    reagents_text, catalysts_text, solvents_text, extra_conditions = _split_reagents(
        _coalesce(sch.get("reagents"), sch.get("agents"), sch.get("other_reagents"))
    )
    temp_text, time_text, cond_from_details, notes_from_details = _extract_conditions_from_details(
        sch.get("reaction_details")
    )
    conditions_bits = []
    for x in [
        extra_conditions,
        cond_from_details,
        _join_texts(sch.get("reaction_conditions")),
        _join_texts(sch.get("conditions")),
        _join_texts(sch.get("other_conditions")),
    ]:
        if x:
            conditions_bits.append(x)
    notes_bits = []
    for x in [
        sch.get("notes"),
        sch.get("other_info"),
        _join_texts(sch.get("annotations")),
        notes_from_details,
        sch.get("description"),
        sch.get("reaction_summary"),
        sch.get("final_product_of_synthesis"),
        _join_texts(sch.get("transition_states")),
    ]:
        if x:
            notes_bits.append(str(x))
    transformation = _coalesce(
        sch.get("transformation_text"),
        sch.get("reaction_name"),
        sch.get("scheme_name"),
        sch.get("reaction_type"),
        sch.get("scheme_text"),
        sch.get("reaction_string"),
        sch.get("description"),
    )
    reactants_text = _join_texts(sch.get("reactants")) or None
    products_text = _join_texts(sch.get("products")) or None
    intermediates_text = _join_texts(_coalesce(sch.get("intermediates"), sch.get("intermediate_species"), sch.get("intermediate_products"))) or None
    yield_text = _join_texts(_coalesce(sch.get("yield"), sch.get("yields"))) or None
    if isinstance(sch.get("products"), list):
        yparts=[]
        for p in sch.get("products"):
            if isinstance(p, dict) and p.get("yield") not in (None, ""):
                nm = p.get("name") or p.get("product_name") or p.get("smiles") or "product"
                yparts.append(f"{nm}: {p.get('yield')}")
        if yparts:
            yt = "; ".join(yparts)
            yield_text = yt if not yield_text else f"{yield_text}; {yt}"
    return {
        "extract_kind": extract_kind,
        "reaction_family_name": page_family,
        "transformation_text": str(transformation) if transformation else None,
        "reactants_text": reactants_text,
        "products_text": products_text,
        "intermediates_text": intermediates_text,
        "reagents_text": reagents_text or None,
        "catalysts_text": catalysts_text or None,
        "solvents_text": solvents_text or None,
        "temperature_text": temp_text,
        "time_text": time_text,
        "yield_text": yield_text,
        "workup_text": None,
        "conditions_text": "; ".join([x for x in conditions_bits if x]) or None,
        "notes_text": "; ".join([x for x in notes_bits if x]) or None,
        "reactant_smiles": None,
        "product_smiles": None,
        "smiles_confidence": 0.0,
        "extraction_confidence": float(sch.get("confidence") or 0.75),
    }


def normalize_response(raw, local_kind: str = None) -> dict:
    """Gemini 응답을 항상 유효한 dict로 정규화하고, 비표준 schema도 extracts[]로 보정."""
    if isinstance(raw, list):
        raw = raw[0] if raw and isinstance(raw[0], dict) else {}
    if not isinstance(raw, dict):
        raw = {}
    result = dict(REQUIRED_KEYS)
    result.update(raw)
    valid_kinds = {"toc","meta_explanatory","reaction_content","unknown"}
    if result["page_kind"] not in valid_kinds:
        result["page_kind"] = "unknown"
    if local_kind in META_PAGE_KINDS:
        result["page_kind"] = local_kind
    if not isinstance(result["scheme_candidates"], list):
        result["scheme_candidates"] = []
    if not isinstance(result["page_section_hints"], list):
        result["page_section_hints"] = []
    if result["page_kind"] in META_PAGE_KINDS:
        result["scheme_candidates"] = []
        result["reaction_family_name"] = None
    if result["reaction_family_name"]:
        result["reaction_family_name"] = canonical_family(result["reaction_family_name"])

    page_family = result["reaction_family_name"]
    page_hints = result.get("page_section_hints") or []
    cleaned = []
    for idx, sc in enumerate(result["scheme_candidates"], start=1):
        if not isinstance(sc, dict):
            continue
        sc = dict(sc)
        sc.setdefault("scheme_index", idx)
        sc.setdefault("confidence", float(sc.get("confidence") or 0.75))
        sc.setdefault("section_type", _infer_section_type(sc, page_hints))
        sc.setdefault("scheme_role", _infer_scheme_role(sc.get("section_type")))
        extracts = sc.get("extracts")
        if not isinstance(extracts, list):
            extracts = []
        if not extracts:
            auto_ext = _build_auto_extract(sc, page_family, page_hints)
            # if we have at least some substance, keep it
            substance = any(auto_ext.get(k) for k in [
                "transformation_text","reactants_text","products_text","reagents_text",
                "conditions_text","notes_text"
            ])
            extracts = [auto_ext] if substance else []
        cleaned_exts = []
        for ext in extracts:
            if not isinstance(ext, dict):
                continue
            ext = dict(ext)
            if page_family:
                ext["reaction_family_name"] = page_family
            elif ext.get("reaction_family_name"):
                ext["reaction_family_name"] = canonical_family(ext["reaction_family_name"])
            sc_val = float(ext.get("smiles_confidence") or 0.0)
            if sc_val < SMILES_MIN_CONF:
                ext["reactant_smiles"] = None
                ext["product_smiles"] = None
                ext["smiles_confidence"] = 0.0
            ext.setdefault("extract_kind", _infer_extract_kind(sc.get("section_type") or "overview"))
            ext.setdefault("extraction_confidence", float(sc.get("confidence") or 0.75))
            cleaned_exts.append(ext)
        sc["extracts"] = cleaned_exts
        cleaned.append(sc)
    result["scheme_candidates"] = cleaned
    return result

# ── 유틸 ───────────────────────────────────────────────────────
def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def img_to_b64(data: bytes) -> str:
    return base64.b64encode(data).decode()

def page_no(fname: str) -> int:
    return int(fname.split("_")[-1].replace(".jpg", ""))

def norm_name(s) -> str:
    return str(s).lower().strip().replace("  ", " ") if s else ""

def now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def _nat_key(name: str):
    m = re.match(r"^named reactions(?: \((\d+)\))?\.zip$", name, re.I)
    if not m:
        return (999999, name.lower())
    idx = int(m.group(1)) if m.group(1) else 0
    return (idx, name.lower())


def _zip_candidates_from_dir(d: Path) -> List[Path]:
    if not d.exists() or not d.is_dir():
        return []
    out: List[Path] = []
    for f in d.iterdir():
        if not f.is_file() or f.suffix.lower() != ".zip":
            continue
        n = f.name.lower()
        if re.match(r"^named reactions(?: \((\d+)\))?\.zip$", n) or re.match(r"^named_reactions(?:_?\d+)?\.zip$", n):
            out.append(f)
    return sorted(out, key=lambda x: _nat_key(x.name))


def find_zip_files(zip_hint: str = None, zip_dir: str = None) -> List[Path]:
    if zip_hint:
        raw = Path(zip_hint)
        variants = [raw]
        if raw.name:
            variants.extend([Path(str(raw).replace("_", " ")), Path(str(raw).replace(" ", "_"))])
        for c in variants:
            if c.exists() and c.is_file():
                return [c]

    search_dirs: List[Path] = []
    if zip_dir:
        search_dirs.append(Path(zip_dir))
    search_dirs.extend([
        Path.cwd(),
        Path(__file__).resolve().parent,
        Path.cwd() / "app" / "data" / "images" / "named reactions",
        Path(__file__).resolve().parent / "app" / "data" / "images" / "named reactions",
    ])

    seen = set()
    ordered_dirs: List[Path] = []
    for d in search_dirs:
        key = str(d.resolve()) if d.exists() else str(d)
        if key in seen:
            continue
        seen.add(key)
        ordered_dirs.append(d)

    for d in ordered_dirs:
        found = _zip_candidates_from_dir(d)
        if found:
            return found

    raise FileNotFoundError(
        "named reactions ZIPs not found. Looked in current folder and app/data/images/named reactions"
    )

# ── 예외 클래스 ────────────────────────────────────────────────
class Api503Error(Exception): pass
class TimeoutError_(Exception): pass
class RecitationError(Exception): pass
class InvalidJsonError(Exception): pass

def classify_error(e: Exception) -> str:
    msg = str(e).lower()
    if "503" in msg or "service unavailable" in msg or "unavailable" in msg:
        return "api_503"
    if "timeout" in msg or "timed out" in msg or "read timeout" in msg:
        return "timeout"
    if "recitation" in msg:
        return "recitation"
    if "json" in msg or "decode" in msg or "parse" in msg:
        return "invalid_json"
    return "unknown"

# ── Gemini 단일 호출 ───────────────────────────────────────────
def _call_once(img_data: bytes, api_key: str, user_prompt: str,
               model: str = MODEL_PRIMARY) -> dict:
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{model}:generateContent?key={api_key}")
    payload = {
        "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": [{"parts": [
            {"inline_data": {"mime_type":"image/jpeg","data":img_to_b64(img_data)}},
            {"text": user_prompt}
        ]}],
        "generationConfig": {"temperature": 0.1,
                             "response_mime_type": "application/json"}
    }
    try:
        r = requests.post(url, json=payload, timeout=120)
    except requests.exceptions.Timeout as e:
        raise TimeoutError_(str(e))
    except requests.exceptions.ConnectionError as e:
        raise TimeoutError_(str(e))

    data = r.json()
    if "error" in data:
        code = data["error"].get("code", 0)
        msg  = data["error"].get("message", "")
        if code == 503 or "unavailable" in msg.lower():
            raise Api503Error(msg)
        raise Exception(f"API {code}: {msg}")

    cands = data.get("candidates", [])
    if not cands:
        raise InvalidJsonError(f"No candidates: {json.dumps(data)[:200]}")

    finish = cands[0].get("finishReason", "")
    if finish == "RECITATION":
        raise RecitationError("RECITATION")
    if finish not in ("STOP", "MAX_TOKENS", ""):
        raise Exception(f"finishReason: {finish}")

    text = cands[0]["content"]["parts"][0]["text"].strip()
    if "```" in text:
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else parts[0]
        if text.startswith("json"):
            text = text[4:].strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise InvalidJsonError(str(e))

# ── 재시도/fallback 포함 Gemini 호출 ───────────────────────────
def gemini_call(img_data: bytes, api_key: str, user_prompt: str,
                local_kind: str = None) -> tuple:
    """
    Returns: (result_dict, attempts, failure_type)
    실패 시 raise Exception
    """
    attempts = 0
    failure_type = "unknown"

    # 시도 순서: [primary/original, primary/original retry,
    #             primary/fallback_prompt, fallback_model/original]
    strategies = [
        (MODEL_PRIMARY,  user_prompt,    0),             # 1차
        (MODEL_PRIMARY,  user_prompt,    SLEEP_RECITATION),  # 2차: 동일
        (MODEL_PRIMARY,  FALLBACK_PROMPT, SLEEP_RECITATION), # 3차: 짧은 프롬프트
        (MODEL_FALLBACK, user_prompt,    SLEEP_RECITATION),  # 4차: fallback model
    ]

    last_err = None
    for model, prompt, sleep_before in strategies:
        if sleep_before > 0:
            time.sleep(sleep_before)
        attempts += 1
        try:
            raw = _call_once(img_data, api_key, prompt, model)
            result = normalize_response(raw, local_kind)
            return result, attempts, None
        except Api503Error as e:
            last_err = e
            failure_type = "api_503"
            # 503 전용 backoff: 5→15→45초
            waits = [SLEEP_503_1, SLEEP_503_2, SLEEP_503_3]
            wait = waits[min(attempts-1, len(waits)-1)]
            print(f"(503 retry {attempts}, wait {wait}s)", end=" ", flush=True)
            time.sleep(wait)
            continue
        except TimeoutError_ as e:
            last_err = e
            failure_type = "timeout"
            wait = SLEEP_TIMEOUT_1 if attempts <= 1 else SLEEP_TIMEOUT_2
            print(f"(timeout retry {attempts}, wait {wait}s)", end=" ", flush=True)
            time.sleep(wait)
            continue
        except RecitationError as e:
            last_err = e
            failure_type = "recitation"
            print(f"(recitation retry {attempts})", end=" ", flush=True)
            continue
        except InvalidJsonError as e:
            last_err = e
            failure_type = "invalid_json"
            continue
        except Exception as e:
            last_err = e
            failure_type = "unknown"
            continue

    raise Exception(f"{failure_type} after {attempts} attempts: {last_err}")

# ── delete-and-rebuild ─────────────────────────────────────────
def delete_batch(conn, batch: str) -> int:
    pi_ids = [r[0] for r in conn.execute(
        "SELECT id FROM page_images WHERE ingest_batch=?", (batch,)).fetchall()]
    if not pi_ids:
        return 0
    ph = ",".join("?"*len(pi_ids))
    sc_ids = [r[0] for r in conn.execute(
        f"SELECT id FROM scheme_candidates WHERE page_image_id IN ({ph})", pi_ids).fetchall()]
    if sc_ids:
        sp = ",".join("?"*len(sc_ids))
        conn.execute(f"DELETE FROM reaction_extracts WHERE scheme_candidate_id IN ({sp})", sc_ids)
    conn.execute(f"DELETE FROM scheme_candidates WHERE page_image_id IN ({ph})", pi_ids)
    conn.execute("DELETE FROM page_images WHERE ingest_batch=?", (batch,))
    conn.execute("DELETE FROM fail_queue WHERE batch=?", (batch,))
    conn.commit()
    return len(pi_ids)

# ── DB insert 헬퍼 ─────────────────────────────────────────────
def insert_page_image(conn, source_zip, pno, fname, sha, fsize, batch, page_kind) -> int:
    ts = now_iso()
    conn.execute("""
        INSERT OR REPLACE INTO page_images
        (source_zip, source_doc, page_no, image_filename,
         sha256, file_size_bytes, ingest_batch, page_kind,
         ingest_status, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (source_zip, SOURCE_DOC, pno, fname, sha, fsize,
          batch, page_kind, "registered", ts, ts))
    conn.commit()
    return conn.execute(
        "SELECT id FROM page_images WHERE source_zip=? AND image_filename=?",
        (source_zip, fname)).fetchone()[0]

def insert_fail_queue(conn, source_zip, pno, fname, page_kind,
                      failure_type, attempts, error_msg, batch):
    conn.execute("""
        INSERT INTO fail_queue
        (source_zip, page_no, image_filename, page_kind,
         failure_type, attempts, last_error_message, batch, created_at)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (source_zip, pno, fname, page_kind,
          failure_type, attempts, error_msg[:500], batch, now_iso()))
    conn.commit()

def insert_schemes(conn, pi_id, result, source_zip, pno):
    """scheme_candidates + reaction_extracts insert. Returns total_extracts."""
    schemes = result.get("scheme_candidates", [])
    page_family = result.get("reaction_family_name")
    page_raw_json = json.dumps(result, ensure_ascii=False)
    total_extracts = 0

    for sch in schemes:
        sidx = sch.get("scheme_index", 1)
        ts = now_iso()
        conn.execute("""
            INSERT OR REPLACE INTO scheme_candidates
            (page_image_id, scheme_index, section_type, scheme_role,
             nearby_text, caption_text, vision_summary, vision_raw_json,
             confidence, review_status,
             detector_model, detector_prompt_version, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,'unreviewed',?,?,?,?)
        """, (pi_id, sidx,
              sch.get("section_type"), sch.get("scheme_role"),
              sch.get("nearby_text"), sch.get("caption_text"),
              sch.get("vision_summary"), page_raw_json,
              sch.get("confidence", 0.5),
              MODEL_PRIMARY, PROMPT_VERSION, ts, ts))
        conn.commit()

        sc_id = conn.execute(
            "SELECT id FROM scheme_candidates WHERE page_image_id=? AND scheme_index=?",
            (pi_id, sidx)).fetchone()["id"]

        extracts = sch.get("extracts") or []
        for ext in extracts:
            rfname = ext.get("reaction_family_name") or page_family
            ts2 = now_iso()
            conn.execute("""
                INSERT INTO reaction_extracts
                (scheme_candidate_id,
                 reaction_family_name, reaction_family_name_norm, extract_kind,
                 transformation_text, reactants_text, products_text, intermediates_text,
                 reagents_text, catalysts_text, solvents_text,
                 temperature_text, time_text, yield_text,
                 workup_text, conditions_text, notes_text,
                 reactant_smiles, product_smiles, smiles_confidence,
                 extraction_confidence, parse_status, promote_decision,
                 extractor_model, extractor_prompt_version,
                 extraction_raw_json, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                sc_id,
                rfname, norm_name(rfname),
                ext.get("extract_kind","unknown"),
                ext.get("transformation_text"),
                ext.get("reactants_text"), ext.get("products_text"),
                ext.get("intermediates_text"), ext.get("reagents_text"),
                ext.get("catalysts_text"), ext.get("solvents_text"),
                ext.get("temperature_text"), ext.get("time_text"),
                ext.get("yield_text"), ext.get("workup_text"),
                ext.get("conditions_text"), ext.get("notes_text"),
                None, None,  # SMILES: normalizer 처리 후 NULL
                0.0, ext.get("extraction_confidence", 0.5),
                "raw", "hold",
                MODEL_PRIMARY, PROMPT_VERSION,
                json.dumps(ext, ensure_ascii=False),
                ts2, ts2
            ))
            total_extracts += 1
        conn.commit()

    return total_extracts

# ── 메인 ───────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode",    choices=["sample","full"], default="sample")
    parser.add_argument("--zip",     default=None)
    parser.add_argument("--zip-dir", default=None)
    parser.add_argument("--db",      default="labint_round9_v5.db")
    parser.add_argument("--batch",   default=None)
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args()

    # batch 기본값
    if args.batch is None:
        args.batch = "sample_batch1" if args.mode == "sample" else "full_batch1"

    load_dotenv(dotenv_path=".env")
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        print("ERROR: GEMINI_API_KEY not found")
        sys.exit(1)

    zip_files = find_zip_files(args.zip or "named reactions.zip", args.zip_dir)
    user_prompt = load_prompt()

    print(f"=== pipeline_named_reactions_v5 ({PIPELINE_VER}) ===")
    if len(zip_files) == 1:
        print(f"mode={args.mode} | ZIP={zip_files[0].name} | DB={args.db}")
    else:
        print(f"mode={args.mode} | ZIPs={len(zip_files)} files | DB={args.db}")
        for zp in zip_files:
            print(f"  - {zp}")
    print(f"prompt={PROMPT_VERSION} | primary={MODEL_PRIMARY}")
    print(f"fallback={MODEL_FALLBACK} | batch={args.batch}")
    print(f"SMILES_MIN_CONF={SMILES_MIN_CONF} | rebuild={args.rebuild}")

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")

    if args.rebuild:
        n = delete_batch(conn, args.batch)
        print(f"[rebuild] Deleted {n} pages from batch '{args.batch}'")

    with zipfile.ZipFile(zip_path) as z:
        all_imgs = sorted(
            [f for f in z.namelist() if f.endswith(".jpg")],
            key=lambda x: page_no(x)
        )

    if args.mode == "sample":
        abbrev  = [f for f in all_imgs if page_no(f) < 46]
        react   = [f for f in all_imgs if page_no(f) >= 46]
        target  = abbrev[:3] + react[:12]
    else:
        target = all_imgs

    print(f"\n처리 대상: {len(target)}장 "
          f"(p{page_no(target[0])}~p{page_no(target[-1])})\n")

    MF_FIELDS = ["page_no","image_filename","page_kind","local_classified",
                 "status","scheme_count","extract_count","reaction_family_name","error"]
    FQ_FIELDS = ["page_no","image_filename","page_kind","failure_type",
                 "attempts","last_error_message","source_zip","batch","created_at"]

    ok = fail = skip = local_skip = 0
    raw_dump = {}
    manifest_rows = []
    run_at = now_iso()

    zip_cache = {}
    try:
        for zip_obj, fname in target:
            source_zip = zip_obj.name
            if source_zip not in zip_cache:
                zip_cache[source_zip] = zipfile.ZipFile(zip_obj)
            z = zip_cache[source_zip]
            pno = page_no(fname)
            local_kind = local_classify(pno)
            print(f"[{source_zip} | p{pno:3d}] local={local_kind[:4]} ", end="", flush=True)

            # idempotency
            if not args.rebuild:
                ex = conn.execute(
                    "SELECT id FROM page_images "
                    "WHERE source_zip=? AND image_filename=? AND ingest_batch=?",
                    (source_zip, fname, args.batch)).fetchone()
                if ex:
                    print("SKIP")
                    skip += 1
                    continue

            img_data = z.read(fname)
            sha = sha256_bytes(img_data)

            # ── 로컬 선분류 → meta/toc: Gemini 호출 없이 처리 ──
            if local_kind in META_PAGE_KINDS:
                pi_id = insert_page_image(conn, source_zip, pno, fname,
                                          sha, len(img_data), args.batch, local_kind)
                conn.execute(
                    "UPDATE page_images SET ingest_status='parsed', updated_at=? WHERE id=?",
                    (now_iso(), pi_id))
                conn.commit()
                print(f"LOCAL_SKIP (no Gemini)")
                local_skip += 1
                ok += 1
                manifest_rows.append({
                    "source_zip": source_zip, "page_no": pno, "image_filename": fname,
                    "page_kind": local_kind, "local_classified": True,
                    "status": "ok", "scheme_count": 0, "extract_count": 0,
                    "reaction_family_name": None, "error": None
                })
                time.sleep(0.1)  # 최소 대기
                continue

            # ── reaction_content / unknown: Gemini 호출 ──────────
            pi_id = insert_page_image(conn, source_zip, pno, fname,
                                      sha, len(img_data), args.batch, local_kind)
            conn.execute(
                "UPDATE page_images SET page_kind=?, ingest_status='registered', updated_at=? WHERE id=?",
                (local_kind, now_iso(), pi_id))
            conn.commit()

            try:
                result, attempts, _ = gemini_call(img_data, api_key, user_prompt, local_kind)
            except Exception as e:
                err = str(e)[:500]
                ftype = classify_error(e)
                print(f"FAIL({ftype}): {err[:80]}")
                conn.execute(
                    "UPDATE page_images SET ingest_status='error', "
                    "page_kind='unknown', error_message=?, updated_at=? WHERE id=?",
                    (err, now_iso(), pi_id))
                conn.commit()
                insert_fail_queue(conn, source_zip, pno, fname, local_kind,
                                  ftype, 4, err, args.batch)
                fail += 1
                manifest_rows.append({
                    "source_zip": source_zip, "page_no": pno, "image_filename": fname,
                    "page_kind": local_kind, "local_classified": False,
                    "status": "fail", "scheme_count": 0, "extract_count": 0,
                    "reaction_family_name": None, "error": err[:200]
                })
                time.sleep(SLEEP_NORMAL)
                continue

            raw_dump[f"{source_zip}::p{pno}"] = result
            page_kind  = result.get("page_kind", "unknown")
            schemes    = result.get("scheme_candidates", [])
            family     = result.get("reaction_family_name")
            n_schemes  = len(schemes)

            conn.execute(
                "UPDATE page_images SET page_kind=?, ingest_status='ocr_done', updated_at=? WHERE id=?",
                (page_kind, now_iso(), pi_id))
            conn.commit()

            print(f"→ Gemini kind={page_kind} | "
                  f"family={str(family or 'null')[:25]} | schemes={n_schemes}")

            # meta/toc 재확인
            if page_kind in META_PAGE_KINDS or n_schemes == 0:
                conn.execute(
                    "UPDATE page_images SET ingest_status='parsed', updated_at=? WHERE id=?",
                    (now_iso(), pi_id))
                conn.commit()
                ok += 1
                manifest_rows.append({
                    "source_zip": source_zip, "page_no": pno, "image_filename": fname,
                    "page_kind": page_kind, "local_classified": False,
                    "status": "ok", "scheme_count": 0, "extract_count": 0,
                    "reaction_family_name": None, "error": None
                })
                time.sleep(SLEEP_NORMAL)
                continue

            # scheme INSERT
            total_extracts = insert_schemes(conn, pi_id, result, source_zip, pno)

            conn.execute(
                "UPDATE page_images SET ingest_status='parsed', updated_at=? WHERE id=?",
                (now_iso(), pi_id))
            conn.commit()

            ok += 1
            manifest_rows.append({
                "source_zip": source_zip, "page_no": pno, "image_filename": fname,
                "page_kind": page_kind, "local_classified": False,
                "status": "ok", "scheme_count": n_schemes,
                "extract_count": total_extracts,
                "reaction_family_name": family, "error": None
            })
            time.sleep(SLEEP_NORMAL)
    finally:
        for _z in zip_cache.values():
            try:
                _z.close()
            except Exception:
                pass

    # ── 결과 요약 ───────────────────────────────────────────────
    sfx = args.mode
    print(f"\n{'='*60}")
    print(f"완료: ok={ok}(local_skip={local_skip}), fail={fail}, skip={skip}")

    print("\n── page_kind 분포 ──")
    for r in conn.execute(
        "SELECT page_kind, COUNT(*) FROM page_images "
        "WHERE ingest_batch=? GROUP BY page_kind ORDER BY COUNT(*) DESC",
        (args.batch,)).fetchall():
        print(f"  {r[0] or 'null'}: {r[1]}건")

    print("\n── DB 상태 ──")
    for tbl in ["page_images","scheme_candidates","reaction_extracts","fail_queue"]:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  {tbl}: {cnt}")
    rc = conn.execute("SELECT COUNT(*) FROM reaction_cards").fetchone()[0]
    print(f"  reaction_cards: {rc} (변경 없음 ✓)")

    if fail > 0:
        print(f"\n── fail_queue ({fail}건) ──")
        frows = conn.execute(
            "SELECT page_no, failure_type, attempts, last_error_message "
            "FROM fail_queue WHERE batch=? ORDER BY page_no",
            (args.batch,)).fetchall()
        for r in frows:
            print(f"  p{r['page_no']}: {r['failure_type']} "
                  f"(attempts={r['attempts']}) {r['last_error_message'][:60]}")

    print("\n── 반응 추출 요약 ──")
    rows = conn.execute("""
        SELECT pi.source_zip, pi.page_no, re.reaction_family_name,
               sc.section_type, sc.scheme_index, sc.confidence
        FROM reaction_extracts re
        JOIN scheme_candidates sc ON re.scheme_candidate_id = sc.id
        JOIN page_images pi ON sc.page_image_id = pi.id
        WHERE pi.ingest_batch=?
        GROUP BY pi.source_zip, pi.page_no, sc.scheme_index, re.reaction_family_name
        ORDER BY pi.source_zip, pi.page_no, sc.scheme_index
    """, (args.batch,)).fetchall()
    for r in rows:
        print(f"  p{r['page_no']:3d} scheme#{r['scheme_index']} | "
              f"{str(r['reaction_family_name'] or 'N/A'):35s} | "
              f"{str(r['section_type'] or '?'):20s} | conf={r['confidence'] or 0:.2f}")

    conn.close()

    # raw JSON dump
    dump_path = Path(args.db).parent / f"raw_json_dump_{sfx}_v5.json"
    with open(dump_path, "w", encoding="utf-8") as f:
        json.dump({
            "run_at": run_at, "mode": args.mode, "model_primary": MODEL_PRIMARY,
            "model_fallback": MODEL_FALLBACK, "prompt_version": PROMPT_VERSION,
            "pipeline_version": PIPELINE_VER,
            "batch": args.batch, "source_zips": [z.name for z in zip_files],
            "stats": {"ok": ok, "fail": fail, "skip": skip,
                      "local_skip": local_skip},
            "pages": raw_dump
        }, f, ensure_ascii=False, indent=2)
    print(f"\nRaw JSON: {dump_path}")

    # manifest
    mf_path = Path(args.db).parent / f"manifest_{sfx}_v5.csv"
    with open(mf_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MF_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(manifest_rows)
    print(f"Manifest: {mf_path}")

    # fail queue CSV
    fq_path = Path(args.db).parent / f"fail_queue_{sfx}_v5.csv"
    fq_conn = sqlite3.connect(args.db)
    fq_conn.row_factory = sqlite3.Row
    fq_rows = fq_conn.execute(
        "SELECT page_no, image_filename, page_kind, failure_type, "
        "attempts, last_error_message, source_zip, batch, created_at "
        "FROM fail_queue WHERE batch=? ORDER BY page_no",
        (args.batch,)).fetchall()
    fq_conn.close()
    with open(fq_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FQ_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in fq_rows:
            w.writerow(dict(r))
    print(f"Fail queue: {fq_path} ({len(fq_rows)}건)")

    print("\n[중요] reaction_cards는 변경하지 않았습니다.")
    print("[중요] promote는 GPT 검증 후 별도 단계에서만 수행합니다.")

if __name__ == "__main__":
    main()
