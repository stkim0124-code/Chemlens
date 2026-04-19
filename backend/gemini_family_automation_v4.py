"""
gemini_family_automation_v4.py
===============================

ChemLens named-reaction family coverage automation — v4.

v3와의 주요 차이점
------------------
1. shutil 기반 파일 rollback 완전 제거. SQL transaction (BEGIN/COMMIT/ROLLBACK)과
   INSERT ID 추적 기반의 DELETE-rollback만 사용.
2. Deterministic lane 분리. 확실한 후보는 Gemini 호출 없이 규칙으로 처리.
   - exact_pattern_match + manual_rows>=2 + page_kind=canonical_overview
   - 기존 family에 structure evidence를 붙이는 작업
3. Gemini lane은 애매한 후보만. merge/create/hold 판단이 필요한 경우 + 모든 신규 family.
4. 묶음(batch) 단위 benchmark. 기본 5개씩 묶어서 한 번 벤치마크.
   regression이면 DELETE로 묶음 통째 폐기 후 1개씩 개별 재시도.
5. Disk budget: snapshot은 LRU로 상한 (기본 20개). candidate_backup 아예 안 만듦.
6. SMILES sanitation 강화 — RDKit parse 전 pre-sanitize.
7. Gemini fallback: deterministic이 실패하거나 regression을 유발하면 자동으로
   Gemini에 수정 요청 (--gemini-fallback).

사용법
------
# dry-run
python gemini_family_automation_v4.py --plan-only --db app\\labint.db \\
    --stage-db app\\labint_v4_stage.db --report-dir reports\\v4

# 목표치까지 full run
python gemini_family_automation_v4.py \\
    --db app\\labint.db \\
    --stage-db app\\labint_v4_stage.db \\
    --benchmark-file benchmark\\named_reaction_benchmark_v4.json \\
    --family-target 250 \\
    --batch-size 5 \\
    --max-rounds 40 \\
    --disk-budget-gb 10 \\
    --report-dir reports\\v4
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any
import re

# ----------------------------------------------------------------------
# RDKit (SMILES sanitation)
# ----------------------------------------------------------------------
try:
    from rdkit import Chem
    from rdkit import RDLogger
    RDLogger.DisableLog("rdApp.*")
    HAVE_RDKIT = True
except Exception:
    HAVE_RDKIT = False


# ----------------------------------------------------------------------
# Gemini
# ----------------------------------------------------------------------
import requests

GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"
DEFAULT_GEMINI_MODELS = ["gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite"]


class GeminiClient:
    def __init__(self, api_key: str, models: list[str], timeout: int = 120):
        self.api_key = api_key
        self.models = models
        self.timeout = timeout
        self._resolved = None

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        last_exc = None
        for attempt in range(3):
            try:
                r = requests.request(method, url, timeout=self.timeout, **kwargs)
                if r.status_code in (429, 500, 502, 503):
                    time.sleep(2 ** attempt)
                    continue
                return r
            except requests.RequestException as e:
                last_exc = e
                time.sleep(2 ** attempt)
        if last_exc:
            raise last_exc
        raise RuntimeError("Gemini request failed after retries")

    def generate_json(self, prompt: str, system_instruction: str | None = None) -> tuple[dict, str]:
        """Generate JSON. Returns (parsed_payload, resolved_model_name)."""
        for model in self.models:
            url = f"{GEMINI_BASE_URL}/models/{model}:generateContent?key={self.api_key}"
            payload = {
                "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.1,
                    "responseMimeType": "application/json",
                },
            }
            if system_instruction:
                payload["systemInstruction"] = {"parts": [{"text": system_instruction}]}
            try:
                r = self._request("POST", url, json=payload)
            except Exception as e:
                print(f"    [gemini] model={model} error: {e}", file=sys.stderr)
                continue
            if r.status_code == 404:
                continue
            if not r.ok:
                print(f"    [gemini] model={model} {r.status_code}: {r.text[:200]}", file=sys.stderr)
                continue
            data = r.json()
            try:
                text = data["candidates"][0]["content"]["parts"][0]["text"]
                json_text = extract_json_text_loose(text) or text
                parsed = json.loads(json_text)
                return parsed, model
            except (KeyError, IndexError, json.JSONDecodeError) as e:
                print(f"    [gemini] model={model} parse error: {e}", file=sys.stderr)
                continue
        raise RuntimeError("All Gemini models failed")


# ----------------------------------------------------------------------
# SMILES sanitation
# ----------------------------------------------------------------------
_NON_SMILES_TOKENS = {
    "", "CH2", "CH3", "OTf", "[H3O+]", "H", "H2", "e-", "hv", "Δ",
    "heat", "Δ heat", "TBAF", "base", "acid", "cat",
}

_PSEUDO_FAMILY_KEYWORDS = (
    "rules", "guidelines", "guideline", "principle", "principles",
    "classification", "classifications", "nomenclature", "terminology",
    "general concepts", "general concept", "appendix", "overview of",
)


def sanitize_smiles(raw: str | None) -> tuple[str | None, str | None]:
    """Sanitize Gemini/OCR 산출물. Returns (canonical_smiles, error_or_None)."""
    if raw is None:
        return None, "null"
    s = raw.strip()
    if not s or s in _NON_SMILES_TOKENS:
        return None, "non_smiles_token"
    if not HAVE_RDKIT:
        return s, None  # fallback: trust as-is

    # salt/counterion 분리 — 가장 큰 organic fragment 선택 (heavy atoms 기준)
    try:
        fragments = s.split(".")
        if len(fragments) > 1:
            # 각 fragment parse해서 heavy atom 수로 선택
            best = None
            best_na = -1
            for f in fragments:
                m = Chem.MolFromSmiles(f, sanitize=False)
                if m is None:
                    continue
                na = m.GetNumHeavyAtoms()
                # 단일 이온이면 skip (1~3 heavy atom)
                if na < 4:
                    continue
                if na > best_na:
                    best_na = na
                    best = f
            if best is None:
                return None, "all_fragments_too_small"
            s = best
    except Exception as e:
        return None, f"fragment_split_error: {e}"

    # RDKit parse
    try:
        mol = Chem.MolFromSmiles(s, sanitize=False)
        if mol is None:
            return None, "parse_failed"
        problems = Chem.DetectChemistryProblems(mol)
        if problems:
            return None, f"chemistry_problems: {[p.GetType() for p in problems]}"
        Chem.SanitizeMol(mol)
        canonical = Chem.MolToSmiles(mol)
        return canonical, None
    except Exception as e:
        return None, f"sanitize_error: {str(e)[:80]}"


def is_pseudo_family_name(name: str | None) -> bool:
    s = (name or '').strip().lower()
    if not s:
        return True
    return any(k in s for k in _PSEUDO_FAMILY_KEYWORDS)


def extract_json_text_loose(text: str) -> str | None:
    s = (text or '').strip()
    if not s:
        return None
    if s.startswith('```'):
        s = re.sub(r'^```(?:json)?\s*', '', s)
        s = re.sub(r'\s*```$', '', s)
        s = s.strip()
    if s[:1] in '{[':
        return s
    m = re.search(r'(\{.*\}|\[.*\])', s, flags=re.DOTALL)
    return m.group(1).strip() if m else None


def normalize_gemini_payload(payload: Any, family_name: str) -> tuple[dict | None, str | None]:
    fam_norm = ''.join(ch.lower() for ch in (family_name or '') if ch.isalnum())

    if isinstance(payload, str):
        jtxt = extract_json_text_loose(payload)
        if not jtxt:
            return None, 'no_json_text'
        try:
            payload = json.loads(jtxt)
        except Exception as e:
            return None, f'json_load_failed: {e}'

    if isinstance(payload, dict):
        if 'decision' in payload:
            return payload, None
        for key in ('item', 'result', 'payload'):
            v = payload.get(key)
            if isinstance(v, dict) and 'decision' in v:
                return v, None
            if isinstance(v, list):
                return normalize_gemini_payload(v, family_name)
        return None, 'dict_missing_decision'

    if isinstance(payload, list):
        dict_items = [x for x in payload if isinstance(x, dict)]
        if not dict_items:
            return None, 'list_without_dict_items'
        if len(dict_items) == 1:
            return normalize_gemini_payload(dict_items[0], family_name)
        exact = []
        for item in dict_items:
            rfn = item.get('reaction_family_name')
            if isinstance(rfn, str):
                if ''.join(ch.lower() for ch in rfn if ch.isalnum()) == fam_norm:
                    exact.append(item)
        if len(exact) == 1:
            return normalize_gemini_payload(exact[0], family_name)
        promote_items = [x for x in dict_items if str(x.get('decision', '')).strip().lower() == 'promote']
        if len(promote_items) == 1:
            return normalize_gemini_payload(promote_items[0], family_name)
        return None, f'ambiguous_list_payload:{len(dict_items)}'

    return None, f'unsupported_payload_type:{type(payload).__name__}'


# ----------------------------------------------------------------------
# DB helpers
# ----------------------------------------------------------------------
def db_connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def get_state(conn: sqlite3.Connection) -> dict:
    cur = conn.cursor()
    return {
        "queryable": cur.execute(
            "SELECT COUNT(*) FROM extract_molecules WHERE queryable=1"
        ).fetchone()[0],
        "tier1": cur.execute(
            "SELECT COUNT(*) FROM extract_molecules WHERE queryable=1 AND quality_tier=1"
        ).fetchone()[0],
        "family_coverage": cur.execute(
            "SELECT COUNT(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1"
        ).fetchone()[0],
        "reaction_extracts": cur.execute(
            "SELECT COUNT(*) FROM reaction_extracts"
        ).fetchone()[0],
        "extract_molecules_total": cur.execute(
            "SELECT COUNT(*) FROM extract_molecules"
        ).fetchone()[0],
    }



# ----------------------------------------------------------------------
# Existing seed reuse helpers
# ----------------------------------------------------------------------
def find_existing_seed_extract_id(conn: sqlite3.Connection, family_name: str) -> int | None:
    """Return a reusable existing extract id backed by role-resolved molecules.

    We deliberately inspect extract_molecules instead of reaction_extracts.reactant_smiles /
    product_smiles because legacy rows often store those text fields as NULL even when the
    role-level molecules are present and queryable.
    """
    row = conn.execute(
        """
        SELECT re.id
        FROM reaction_extracts re
        WHERE re.reaction_family_name=?
          AND EXISTS (
                SELECT 1 FROM extract_molecules em
                WHERE em.extract_id=re.id
                  AND LOWER(TRIM(em.role))='reactant'
                  AND COALESCE(TRIM(em.smiles), '') <> ''
          )
          AND EXISTS (
                SELECT 1 FROM extract_molecules em
                WHERE em.extract_id=re.id
                  AND LOWER(TRIM(em.role))='product'
                  AND COALESCE(TRIM(em.smiles), '') <> ''
          )
        ORDER BY CASE WHEN re.extract_kind='canonical_overview' THEN 0 ELSE 1 END,
                 CASE WHEN COALESCE(re.parse_status,'') IN (
                        'gemini_auto_seed', 'deterministic_gemini_seed',
                        'deterministic_seed_from_existing', 'manual_curated_seed',
                        'vision_raw_json_promote', 'rdkit_direct_parse', 'pubchem_name_lookup'
                 ) THEN 0 ELSE 1 END,
                 re.id ASC
        LIMIT 1
        """,
        (family_name,),
    ).fetchone()
    return int(row[0]) if row else None


def _split_smiles_field(raw: str | None) -> list[dict[str, float]]:
    """Legacy helper kept for compatibility.

    reaction_extracts text fields in this project often use ' | ' between molecules, while
    a literal '.' can be part of a valid salt/counterion SMILES. Prefer pipe-splitting when
    present and otherwise keep the whole field intact.
    """
    raw = (raw or '').strip()
    if not raw:
        return []
    parts = [p.strip() for p in raw.split('|')] if '|' in raw else [raw]
    return [{"smiles": p, "confidence": 1.0} for p in parts if p]


def build_payload_from_existing_extract(conn: sqlite3.Connection, extract_id: int, family_name: str) -> dict | None:
    row = conn.execute(
        """
        SELECT id, scheme_candidate_id, extract_kind, transformation_text, reactants_text,
               products_text, intermediates_text, reagents_text, catalysts_text, solvents_text,
               temperature_text, time_text, yield_text, workup_text, conditions_text,
               notes_text, extractor_model, extractor_prompt_version, extraction_raw_json
        FROM reaction_extracts
        WHERE id=?
        """,
        (extract_id,),
    ).fetchone()
    if row is None:
        return None

    buckets = {"reactants": [], "products": [], "reagents": [], "intermediates": []}
    role_map = {
        'reactant': 'reactants', 'product': 'products',
        'reagent': 'reagents', 'intermediate': 'intermediates'
    }
    for em in conn.execute(
        """
        SELECT role, smiles, role_confidence
        FROM extract_molecules
        WHERE extract_id=?
          AND COALESCE(TRIM(smiles), '') <> ''
        ORDER BY CASE LOWER(TRIM(role))
                   WHEN 'reactant' THEN 0
                   WHEN 'product' THEN 1
                   WHEN 'reagent' THEN 2
                   WHEN 'intermediate' THEN 3
                   ELSE 9 END,
                 id ASC
        """,
        (extract_id,),
    ):
        role = (em["role"] or '').strip().lower()
        bucket = role_map.get(role)
        if not bucket:
            continue
        buckets[bucket].append({
            "smiles": em["smiles"],
            "confidence": float(em["role_confidence"] or 1.0),
        })

    if not buckets['reactants'] or not buckets['products']:
        return None

    notes = row["notes_text"] or row["transformation_text"] or f"Reused existing seed extract {extract_id} for {family_name}."
    return {
        "decision": "promote",
        "confidence": 0.99,
        "reaction_family_name": family_name,
        "reactants": buckets['reactants'],
        "products": buckets['products'],
        "reagents": buckets['reagents'],
        "intermediates": buckets['intermediates'],
        "notes_text": str(notes)[:500],
        "is_generic_risky": False,
        "_reuse_extract_meta": {
            "source_extract_id": int(row["id"]),
            "scheme_candidate_id": int(row["scheme_candidate_id"]),
            "extract_kind": row["extract_kind"],
            "transformation_text": row["transformation_text"],
            "reactants_text": row["reactants_text"],
            "products_text": row["products_text"],
            "intermediates_text": row["intermediates_text"],
            "reagents_text": row["reagents_text"],
            "catalysts_text": row["catalysts_text"],
            "solvents_text": row["solvents_text"],
            "temperature_text": row["temperature_text"],
            "time_text": row["time_text"],
            "yield_text": row["yield_text"],
            "workup_text": row["workup_text"],
            "conditions_text": row["conditions_text"],
            "extractor_model": row["extractor_model"],
            "extractor_prompt_version": row["extractor_prompt_version"],
            "extraction_raw_json": row["extraction_raw_json"],
        },
    }


# ----------------------------------------------------------------------
# Candidate selection
# ----------------------------------------------------------------------
@dataclass
class Candidate:
    family_name: str
    family_name_norm: str = ""
    page_no: int | None = None
    page_label: str | None = None
    page_kind: str | None = None
    image_filename: str | None = None
    page_title: str | None = None
    page_section_name: str | None = None
    page_summary: str | None = None
    page_notes: str | None = None
    manual_rows: int = 0
    extract_rows: int = 0
    priority_score: int = 0
    similar_top_score: float = 0.0
    similar_top_name: str | None = None
    exact_pattern_family: str | None = None
    existing_queryable_count: int = 0
    seed_extract_id: int | None = None
    seed_ready: bool = False
    lane: str = "unknown"  # "deterministic" | "gemini"


def select_candidates(conn: sqlite3.Connection, limit: int,
                      exclude: set[str], include_covered_families: bool = False,
                      covered_skip_threshold: int = 1) -> list[Candidate]:
    """
    Uncovered or under-covered named-reaction family 후보 선출.
    기본 전략:
      1. 현재 queryable evidence가 전혀 없는 family 우선
      2. canonical_overview + manual rows 많은 family 우선
      3. 이미 coverage에 기여한 family는 기본적으로 제외 (효율 우선)
    """
    covered_raw = {r[0] for r in conn.execute(
        "SELECT DISTINCT reaction_family_name FROM extract_molecules WHERE queryable=1"
    )}
    covered_counts = {}
    for row in conn.execute("""
        SELECT reaction_family_name, COUNT(*) FROM extract_molecules
        WHERE queryable=1 GROUP BY reaction_family_name
    """):
        covered_counts[_norm(row[0])] = row[1]

    registry = list(conn.execute("""
        SELECT family_name, family_name_norm FROM reaction_family_patterns
        ORDER BY family_name
    """))
    candidates: list[Candidate] = []
    for reg in registry:
        fname = reg[0]
        # IMPORTANT: always use runtime normalization for coverage lookup.
        # reaction_family_patterns.family_name_norm may contain legacy punctuation/hyphen variants.
        fnorm = _norm(fname)
        existing_queryable_count = covered_counts.get(fnorm, 0)
        if fnorm in exclude:
            continue
        if is_pseudo_family_name(fname):
            continue
        if (not include_covered_families) and existing_queryable_count >= covered_skip_threshold:
            continue
        if existing_queryable_count >= 5:
            continue

        rows = list(conn.execute("""
            SELECT page_no, page_label, page_kind, image_filename, reference_family_name, family_names,
                   title, section_name, summary, notes
            FROM manual_page_knowledge
            WHERE reference_family_name=? OR family_names LIKE ?
        """, (fname, f"%{fname}%")))
        if not rows:
            continue
        manual_count = len(rows)
        best_page = None
        for r in rows:
            if r["page_kind"] == "canonical_overview":
                best_page = r
                break
        if best_page is None:
            best_page = rows[0]
        if is_pseudo_family_name(best_page["title"]) or is_pseudo_family_name(best_page["section_name"]):
            continue

        extract_rows = conn.execute(
            "SELECT COUNT(*) FROM reaction_extracts WHERE reaction_family_name=?",
            (fname,)
        ).fetchone()[0]
        seed_extract_id = find_existing_seed_extract_id(conn, fname)
        seed_ready = seed_extract_id is not None

        pscore = manual_count * 3 + (2 if best_page["page_kind"] == "canonical_overview" else 0)
        pscore -= existing_queryable_count * 6
        pscore -= min(extract_rows, 10)
        if seed_ready:
            pscore += 4

        cand = Candidate(
            family_name=fname,
            family_name_norm=fnorm,
            page_no=best_page["page_no"],
            page_label=best_page["page_label"],
            page_kind=best_page["page_kind"],
            image_filename=best_page["image_filename"],
            page_title=best_page["title"],
            page_section_name=best_page["section_name"],
            page_summary=best_page["summary"],
            page_notes=best_page["notes"],
            manual_rows=manual_count,
            extract_rows=extract_rows,
            priority_score=pscore,
            similar_top_score=1.0 if fname in covered_raw else 0.0,
            similar_top_name=fname if fname in covered_raw else None,
            exact_pattern_family=fname,
            existing_queryable_count=existing_queryable_count,
            seed_extract_id=seed_extract_id,
            seed_ready=seed_ready,
        )
        candidates.append(cand)

    seen_norm = set()
    seen_name = set()
    unique: list[Candidate] = []
    for c in candidates:
        key_norm = _norm(c.family_name)
        key_name = c.family_name.strip().lower()
        if key_norm in seen_norm or key_name in seen_name:
            continue
        seen_norm.add(key_norm)
        seen_name.add(key_name)
        c.family_name_norm = key_norm
        unique.append(c)
    candidates = unique

    candidates.sort(
        key=lambda c: (
            c.seed_ready is False,
            c.existing_queryable_count > 0,
            c.extract_rows > 0,
            -c.priority_score,
            c.family_name,
        )
    )

    for c in candidates:
        if (c.seed_ready
                and c.manual_rows >= 2
                and c.page_kind == "canonical_overview"):
            c.lane = "deterministic"
        else:
            c.lane = "gemini"

    return candidates[:limit]


def _norm(s: str) -> str:
    return "".join(ch.lower() for ch in (s or "") if ch.isalnum())


def is_discovery_frontier(candidates: list[Candidate]) -> bool:
    if not candidates:
        return False
    top = candidates[: min(10, len(candidates))]
    return all((c.lane == 'gemini' and not c.seed_ready and c.extract_rows == 0) for c in top)


# ----------------------------------------------------------------------
# Gemini prompt for family seed generation
# ----------------------------------------------------------------------
GEMINI_SYSTEM = """You are a synthetic-organic-chemistry curator building a named-reaction knowledge base.
Your job: given a named reaction, produce clean, verifiable SMILES for a textbook example of that reaction.

Rules:
- Return JSON only, matching the schema provided.
- Use canonical, RDKit-parseable SMILES (no made-up atoms, no OCR artifacts).
- For named reactions, prefer the textbook archetype, not an obscure variant.
- If you are not confident, set decision="hold" and explain why.
- Never fabricate. If you don't know the reaction, set decision="reject".
- Prefer organic fragments over counterions. Salts may be included via "." but organic fragment must come first.
"""

GEMINI_PROMPT_TEMPLATE = """Target family: {family_name}
Page reference: p{page_no} ({page_label}), kind={page_kind}
Image file hint: {image_filename}
Page title: {page_title}
Section name: {page_section_name}
Summary: {page_summary}
Notes: {page_notes}
Manual rows available: {manual_rows}
Existing queryable molecules for this family: {existing_queryable_count}
Existing reaction_extract rows for this family: {extract_rows}
Already covered by canonical DB: {covered}

Generate ONE canonical textbook example of {family_name}. Prefer the archetype implied by the summary/notes.
Return ONE JSON OBJECT only. Do NOT return a list or multiple alternatives.

{{
  "decision": "promote" | "hold" | "reject",
  "confidence": 0.0-1.0,
  "reaction_family_name": "{family_name}",
  "reactants": [{{"smiles": "...", "confidence": 0.0-1.0}}],
  "products": [{{"smiles": "...", "confidence": 0.0-1.0}}],
  "reagents": [{{"smiles": "...", "confidence": 0.0-1.0}}],
  "intermediates": [{{"smiles": "...", "confidence": 0.0-1.0}}],
  "notes_text": "textbook description of the transformation",
  "is_generic_risky": true | false
}}

Only "promote" triggers an insert. Keep confidence honest.
"""


def call_gemini_for_candidate(gemini: GeminiClient, cand: Candidate) -> tuple[dict | None, str, str | None]:
    """Returns (normalized_payload, model_name, error)."""
    prompt = GEMINI_PROMPT_TEMPLATE.format(
        family_name=cand.family_name,
        page_no=cand.page_no or "?",
        page_label=cand.page_label or "?",
        page_kind=cand.page_kind or "?",
        image_filename=cand.image_filename or "?",
        page_title=(cand.page_title or "?")[:200],
        page_section_name=(cand.page_section_name or "?")[:200],
        page_summary=(cand.page_summary or "?")[:600],
        page_notes=(cand.page_notes or "?")[:400],
        manual_rows=cand.manual_rows,
        existing_queryable_count=cand.existing_queryable_count,
        extract_rows=cand.extract_rows,
        covered="yes" if cand.similar_top_score >= 0.99 else "no",
    )
    try:
        payload, model = gemini.generate_json(prompt, GEMINI_SYSTEM)
        norm_payload, err = normalize_gemini_payload(payload, cand.family_name)
        if err:
            return None, model, err
        return norm_payload, model, None
    except Exception as e:
        return None, "", str(e)[:200]


# ----------------------------------------------------------------------
# INSERT logic (shared by deterministic / gemini lanes)
# ----------------------------------------------------------------------
@dataclass
class InsertResult:
    success: bool
    reaction_extract_id: int | None = None
    molecule_ids: list[int] = field(default_factory=list)
    error: str | None = None
    sanitation_failures: list[str] = field(default_factory=list)


def insert_family_seed(conn: sqlite3.Connection, cand: Candidate,
                       payload: dict, structure_source: str,
                       model_name: str = "") -> InsertResult:
    """Insert reaction_extract + extract_molecules for a family seed.
    Caller must manage transaction (BEGIN/COMMIT/ROLLBACK) around this.
    Returns InsertResult with inserted IDs for later rollback.
    """
    buckets = {
        "reactant": payload.get("reactants") or [],
        "product": payload.get("products") or [],
        "reagent": payload.get("reagents") or [],
        "intermediate": payload.get("intermediates") or [],
    }
    cleaned: dict[str, list[tuple[str, float]]] = {k: [] for k in buckets}
    sanitation_failures: list[str] = []
    for role, items in buckets.items():
        for item in items:
            raw = item.get("smiles") if isinstance(item, dict) else str(item)
            conf = item.get("confidence", 1.0) if isinstance(item, dict) else 1.0
            canonical, err = sanitize_smiles(raw)
            if canonical is None:
                sanitation_failures.append(f"{role}:'{raw}' ({err})")
                continue
            cleaned[role].append((canonical, conf))

    if not cleaned["reactant"] or not cleaned["product"]:
        return InsertResult(
            success=False,
            error=f"no valid reactant/product after sanitation (failures: {sanitation_failures})",
            sanitation_failures=sanitation_failures,
        )

    notes_text = str(payload.get("notes_text") or "")[:500]
    reuse_meta = payload.get("_reuse_extract_meta") if isinstance(payload, dict) else None

    reactant_smiles_text = " | ".join(s for s, _ in cleaned["reactant"])
    product_smiles_text = " | ".join(s for s, _ in cleaned["product"])
    reactants_text = reuse_meta.get("reactants_text") if reuse_meta else None
    products_text = reuse_meta.get("products_text") if reuse_meta else None
    reagents_text = reuse_meta.get("reagents_text") if reuse_meta else None
    intermediates_text = reuse_meta.get("intermediates_text") if reuse_meta else None
    catalysts_text = reuse_meta.get("catalysts_text") if reuse_meta else None
    solvents_text = reuse_meta.get("solvents_text") if reuse_meta else None
    temperature_text = reuse_meta.get("temperature_text") if reuse_meta else None
    time_text = reuse_meta.get("time_text") if reuse_meta else None
    yield_text = reuse_meta.get("yield_text") if reuse_meta else None
    workup_text = reuse_meta.get("workup_text") if reuse_meta else None
    conditions_text = reuse_meta.get("conditions_text") if reuse_meta else None
    transformation_text = reuse_meta.get("transformation_text") if reuse_meta else notes_text
    extraction_raw_json = reuse_meta.get("extraction_raw_json") if reuse_meta else json.dumps(payload, ensure_ascii=False)
    scheme_candidate_id = int(reuse_meta.get("scheme_candidate_id", 1)) if reuse_meta else 1
    extract_kind = reuse_meta.get("extract_kind") if reuse_meta else "canonical_overview"

    re_cur = conn.execute("""
        INSERT INTO reaction_extracts (
            scheme_candidate_id,
            reaction_family_name,
            reaction_family_name_norm,
            extract_kind,
            transformation_text,
            reactants_text,
            products_text,
            intermediates_text,
            reagents_text,
            catalysts_text,
            solvents_text,
            temperature_text,
            time_text,
            yield_text,
            workup_text,
            conditions_text,
            notes_text,
            reactant_smiles,
            product_smiles,
            smiles_confidence,
            extraction_confidence,
            parse_status,
            promote_decision,
            rejection_reason,
            extractor_model,
            extractor_prompt_version,
            extraction_raw_json,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        scheme_candidate_id,
        cand.family_name,
        cand.family_name_norm or _norm(cand.family_name),
        extract_kind or "canonical_overview",
        transformation_text,
        reactants_text,
        products_text,
        intermediates_text,
        reagents_text,
        catalysts_text,
        solvents_text,
        temperature_text,
        time_text,
        yield_text,
        workup_text,
        conditions_text,
        notes_text,
        reactant_smiles_text,
        product_smiles_text,
        float(payload.get("confidence", 0.9)),
        float(payload.get("confidence", 0.9)),
        structure_source,
        "promote",
        None,
        model_name or (reuse_meta.get("extractor_model") if reuse_meta else structure_source),
        reuse_meta.get("extractor_prompt_version") if reuse_meta else "v4",
        extraction_raw_json,
        datetime.now().isoformat(),
        datetime.now().isoformat(),
    ))
    re_id = re_cur.lastrowid

    mol_ids: list[int] = []
    for role, items in cleaned.items():
        for smi, conf in items:
            em_cur = conn.execute("""
                INSERT INTO extract_molecules (
                    extract_id,
                    role,
                    smiles,
                    smiles_kind,
                    quality_tier,
                    reaction_family_name,
                    page_no,
                    queryable,
                    note_text,
                    source_field,
                    structure_source,
                    role_confidence,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                re_id,
                role,
                smi,
                "explicit",
                1,
                cand.family_name,
                cand.page_no,
                1 if role in ("reactant", "product") else 0,
                f"[{structure_source}]",
                role,
                structure_source,
                float(conf),
                datetime.now().isoformat(),
            ))
            mol_ids.append(em_cur.lastrowid)

    return InsertResult(
        success=True,
        reaction_extract_id=re_id,
        molecule_ids=mol_ids,
        sanitation_failures=sanitation_failures,
    )


def rollback_insert(conn: sqlite3.Connection, ins: InsertResult) -> None:
    """Explicit DELETE-rollback (caller manages transaction)."""
    if not ins.success:
        return
    if ins.molecule_ids:
        placeholders = ",".join("?" * len(ins.molecule_ids))
        conn.execute(
            f"DELETE FROM extract_molecules WHERE id IN ({placeholders})",
            ins.molecule_ids,
        )
    if ins.reaction_extract_id is not None:
        conn.execute(
            "DELETE FROM reaction_extracts WHERE id = ?",
            (ins.reaction_extract_id,),
        )


# ----------------------------------------------------------------------
# Benchmark
# ----------------------------------------------------------------------
def resolve_benchmark_path(candidate: str | Path) -> Path:
    p = Path(candidate)
    options = []
    if p.is_absolute():
        options.append(p)
    else:
        options.extend([
            Path.cwd() / p,
            Path.cwd() / p.name,
            Path.cwd() / "benchmark" / p.name,
        ])
    for opt in options:
        if opt.exists():
            return opt.resolve()
    return (Path.cwd() / p).resolve()


def run_benchmark(stage_db: Path, benchmark_file: Path,
                  report_dir: Path, suffix: str) -> dict:
    """Run the benchmark against stage_db. Returns summary dict."""
    runner = Path("run_named_reaction_benchmark_small.py")
    if not runner.exists():
        return {"error": f"benchmark runner not found: {runner}"}
    benchmark_file = resolve_benchmark_path(benchmark_file)
    if not benchmark_file.exists():
        return {"error": f"benchmark file not found: {benchmark_file}"}
    env = os.environ.copy()
    env["LABINT_DB_PATH"] = str(stage_db)
    env["PYTHONIOENCODING"] = "utf-8"
    cmd = [
        sys.executable, str(runner),
        "--benchmark", str(benchmark_file),
    ]
    try:
        result = subprocess.run(
            cmd, env=env, capture_output=True, text=True, timeout=300,
            encoding="utf-8", errors="replace",
        )
        summary = {}
        produced_json = None
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith("JSON:"):
                produced_json = line.split("JSON:", 1)[1].strip()
                break
        if produced_json:
            pj = Path(produced_json)
            if not pj.is_absolute():
                pj = (Path.cwd() / pj).resolve()
            if pj.exists():
                try:
                    bench_payload = json.loads(pj.read_text(encoding="utf-8"))
                    summary = bench_payload.get("summary") or {}
                except Exception:
                    summary = {}
        if not summary:
            for line in result.stdout.splitlines():
                line = line.strip()
                if line.startswith("{") and "top1_accuracy" in result.stdout:
                    try:
                        start = result.stdout.index("{")
                        end = result.stdout.index("}", start) + 1
                        summary = json.loads(result.stdout[start:end])
                        break
                    except Exception:
                        continue
        bench_path = report_dir / f"benchmark_{suffix}.json"
        bench_path.write_text(
            json.dumps({"summary": summary, "stdout": result.stdout[-4000:],
                        "stderr": result.stderr[-2000:],
                        "runner_returncode": result.returncode,
                        "benchmark_file": str(benchmark_file)}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        if result.returncode != 0 and not summary:
            return {"error": f"benchmark runner failed rc={result.returncode}"}
        return summary
    except subprocess.TimeoutExpired:
        return {"error": "benchmark timeout"}
    except Exception as e:
        return {"error": str(e)}


def has_regression(baseline: dict, current: dict,
                   allowed_top1_drop: float = 0.0,
                   allowed_violation_increase: int = 0) -> tuple[bool, str]:
    if not baseline or not current:
        return False, "no baseline or current"
    if "error" in current:
        return False, f"current has error: {current['error']}"
    b_top1 = float(baseline.get("top1_accuracy", 0))
    c_top1 = float(current.get("top1_accuracy", 0))
    if (b_top1 - c_top1) > allowed_top1_drop + 1e-9:
        return True, f"top1 drop: {b_top1:.4f} → {c_top1:.4f}"
    b_viol = int(baseline.get("disallow_top3_violations", 0))
    c_viol = int(current.get("disallow_top3_violations", 0))
    if (c_viol - b_viol) > allowed_violation_increase:
        return True, f"violations: {b_viol} → {c_viol}"
    return False, ""


# ----------------------------------------------------------------------
# Disk budget & snapshot management
# ----------------------------------------------------------------------
def dir_size_bytes(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for p in path.rglob("*"):
        if p.is_file():
            try:
                total += p.stat().st_size
            except OSError:
                pass
    return total


def lru_trim_snapshots(report_dir: Path, max_snapshots: int) -> int:
    """Keep only the N most recent snapshot_after_*.db files. Returns # deleted."""
    snaps = sorted(
        report_dir.glob("snapshot_after_*.db"),
        key=lambda p: p.stat().st_mtime,
    )
    if len(snaps) <= max_snapshots:
        return 0
    to_delete = snaps[:-max_snapshots]
    for p in to_delete:
        try:
            p.unlink()
        except OSError:
            pass
    return len(to_delete)


def check_disk_budget(report_dir: Path, budget_gb: float) -> tuple[bool, float]:
    size_gb = dir_size_bytes(report_dir) / (1024 ** 3)
    return size_gb < budget_gb, size_gb


# ----------------------------------------------------------------------
# Main run loop
# ----------------------------------------------------------------------
def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default="app/labint.db",
                   help="Canonical DB (read-only; source to clone)")
    p.add_argument("--stage-db", default="app/labint_v4_stage.db",
                   help="Stage DB (writable copy)")
    p.add_argument("--benchmark-file",
                   default="benchmark/named_reaction_benchmark_gate.json",
                   help="Gate benchmark JSON file (used for rollback / acceptance)")
    p.add_argument("--diagnostic-benchmark-file",
                   default="benchmark/named_reaction_benchmark_v4.json",
                   help="Optional diagnostic benchmark JSON file (baseline/final only)")
    p.add_argument("--report-dir", default="reports/v4",
                   help="Per-run report directory root")
    p.add_argument("--reset-stage", action="store_true",
                   help="Overwrite stage-db from canonical before start")
    p.add_argument("--family-target", type=int, default=250,
                   help="Stop when family_coverage reaches this")
    p.add_argument("--batch-size", type=int, default=5,
                   help="Candidates per benchmark check batch")
    p.add_argument("--max-rounds", type=int, default=40)
    p.add_argument("--max-empty-rounds", type=int, default=3,
                   help="Consecutive empty rounds before stop")
    p.add_argument("--candidate-limit", type=int, default=30,
                   help="Candidates per round to evaluate")
    p.add_argument("--disk-budget-gb", type=float, default=10.0)
    p.add_argument("--max-snapshots", type=int, default=20)
    p.add_argument("--snapshot-every", type=int, default=25,
                   help="Snapshot every N successful inserts")
    p.add_argument("--deterministic-only", action="store_true",
                   help="Skip Gemini lane entirely (fastest)")
    p.add_argument("--gemini-only", action="store_true",
                   help="Skip deterministic lane")
    p.add_argument("--gemini-fallback", default="yes",
                   help="When deterministic fails or rollbacks, call Gemini "
                        "to retry that candidate (yes/no)")
    p.add_argument("--allowed-top1-drop", type=float, default=0.0)
    p.add_argument("--allowed-violation-increase", type=int, default=0)
    p.add_argument("--plan-only", action="store_true",
                   help="Show candidate plan and exit")
    p.add_argument("--min-confidence", type=float, default=0.75)
    p.add_argument("--gemini-api-key", default=None,
                   help="Override GEMINI_API_KEY env var")
    p.add_argument("--gemini-models", default=",".join(DEFAULT_GEMINI_MODELS))
    p.add_argument("--include-covered-families", action="store_true",
                   help="Include families that already contribute to current family coverage")
    p.add_argument("--covered-skip-threshold", type=int, default=1,
                   help="If include-covered-families is false, skip families with >= this many queryable molecules")
    p.add_argument("--min-baseline-top1", type=float, default=0.95)
    p.add_argument("--min-baseline-top3", type=float, default=0.95)
    p.add_argument("--discovery-candidate-limit", type=int, default=3,
                   help="When frontier is discovery-only, try at most this many candidates per round")
    p.add_argument("--zero-progress-stop-rounds", type=int, default=1,
                   help="Stop after this many consecutive rounds with inserted=0")
    args = p.parse_args()

    canonical_db = Path(args.db).resolve()
    stage_db = Path(args.stage_db).resolve()
    benchmark_file = resolve_benchmark_path(args.benchmark_file)
    diagnostic_benchmark_file = resolve_benchmark_path(args.diagnostic_benchmark_file) if args.diagnostic_benchmark_file else None

    if not canonical_db.exists():
        print(f"[FATAL] canonical db not found: {canonical_db}")
        return 1
    if canonical_db == stage_db:
        print("[FATAL] canonical and stage must be different paths")
        return 1

    # Setup stage DB
    if args.reset_stage or not stage_db.exists():
        print(f"[setup] cloning canonical → stage: {stage_db}")
        shutil.copy2(canonical_db, stage_db)

    # Setup report dir
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = Path(args.report_dir) / run_id
    report_dir.mkdir(parents=True, exist_ok=True)
    print(f"[setup] report dir: {report_dir}")
    print(f"[setup] benchmark file: {benchmark_file}")

    # Gemini
    api_key = args.gemini_api_key or os.environ.get("GEMINI_API_KEY", "")
    gemini = None
    if not args.deterministic_only:
        if not api_key:
            # try to read .env — with robust encoding fallback
            env_path = Path(".env")
            if env_path.exists():
                env_text = None
                # 여러 인코딩을 순차 시도 (Windows 한글 환경 대비)
                for enc in ("utf-8", "utf-8-sig", "cp949", "latin-1"):
                    try:
                        env_text = env_path.read_text(encoding=enc)
                        break
                    except (UnicodeDecodeError, OSError):
                        continue
                if env_text is None:
                    # 마지막 수단: 바이너리로 읽어서 ASCII 라인만 찾기
                    try:
                        raw = env_path.read_bytes()
                        env_text = raw.decode("ascii", errors="ignore")
                    except OSError:
                        env_text = ""
                for line in env_text.splitlines():
                    if line.startswith("GEMINI_API_KEY="):
                        api_key = line.split("=", 1)[1].strip().strip('"\'')
                        break
        if not api_key:
            print("[warn] No GEMINI_API_KEY found. Gemini lane disabled.")
        else:
            models = [m.strip() for m in args.gemini_models.split(",") if m.strip()]
            gemini = GeminiClient(api_key, models)

    # Connect
    conn = db_connect(stage_db)
    start_state = get_state(conn)
    print(f"[state] start: {start_state}")

    # Baseline benchmark
    baseline_bench = None
    diagnostic_baseline = None
    if not args.plan_only:
        print("[benchmark] baseline...")
        baseline_bench = run_benchmark(stage_db, benchmark_file, report_dir, "baseline")
        print(f"  top1={baseline_bench.get('top1_accuracy')} "
              f"top3={baseline_bench.get('top3_accuracy')} "
              f"violations={baseline_bench.get('disallow_top3_violations')}")
        if "error" in baseline_bench:
            print(f"[FATAL] baseline benchmark error: {baseline_bench['error']}")
            conn.close()
            return 2
        if float(baseline_bench.get("top1_accuracy", 0.0)) < args.min_baseline_top1:
            print(f"[FATAL] baseline top1 below floor: {baseline_bench.get('top1_accuracy')} < {args.min_baseline_top1}")
            conn.close()
            return 2
        if float(baseline_bench.get("top3_accuracy", 0.0)) < args.min_baseline_top3:
            print(f"[FATAL] baseline top3 below floor: {baseline_bench.get('top3_accuracy')} < {args.min_baseline_top3}")
            conn.close()
            return 2
        if diagnostic_benchmark_file and diagnostic_benchmark_file.exists() and diagnostic_benchmark_file != benchmark_file:
            diagnostic_baseline = run_benchmark(stage_db, diagnostic_benchmark_file, report_dir, "diagnostic_baseline")
            noisy_cases = diagnostic_baseline.get("noisy_cases")
            meaningful_cases = diagnostic_baseline.get("meaningful_cases")
            noisy_pass = diagnostic_baseline.get("noisy_pass")
            meaningful_pass = diagnostic_baseline.get("meaningful_pass")
            if noisy_cases is not None or meaningful_cases is not None:
                print(f"  diagnostic noisy={noisy_pass}/{noisy_cases} meaningful={meaningful_pass}/{meaningful_cases}")

    # Run loop
    quarantined: set[str] = set()
    run_items: list[dict] = []
    rounds_run = 0
    consecutive_empty = 0
    total_inserted = 0
    stopped_reason = "completed"
    consecutive_zero_progress = 0

    try:
        while rounds_run < args.max_rounds:
            rounds_run += 1
            print(f"\n=== Round {rounds_run} ===")

            # disk budget check
            ok, size_gb = check_disk_budget(report_dir, args.disk_budget_gb)
            print(f"[disk] report dir size: {size_gb:.2f} GB / {args.disk_budget_gb} GB")
            if not ok:
                lru_trim_snapshots(report_dir, args.max_snapshots // 2)
                ok2, size_gb2 = check_disk_budget(report_dir, args.disk_budget_gb)
                if not ok2:
                    stopped_reason = f"disk_budget_exceeded: {size_gb2:.2f} GB"
                    break

            state = get_state(conn)
            if state["family_coverage"] >= args.family_target:
                stopped_reason = "family_target_reached"
                break

            candidates = select_candidates(
                conn, limit=args.candidate_limit, exclude=quarantined,
                include_covered_families=args.include_covered_families,
                covered_skip_threshold=args.covered_skip_threshold,
            )
            plan_path = report_dir / f"plan_round_{rounds_run:02d}.json"
            plan_path.write_text(
                json.dumps([asdict(c) for c in candidates], indent=2, ensure_ascii=False),
                encoding="utf-8",
            )

            if not candidates:
                consecutive_empty += 1
                print(f"[round {rounds_run}] no candidates "
                      f"(consecutive_empty={consecutive_empty})")
                if consecutive_empty >= args.max_empty_rounds:
                    stopped_reason = "max_empty_rounds"
                    break
                continue

            discovery_mode = is_discovery_frontier(candidates)
            active_candidates = candidates[:args.discovery_candidate_limit] if discovery_mode else candidates

            if args.plan_only:
                print(f"[plan-only] {len(candidates)} candidates:")
                for c in candidates[:20]:
                    print(f"  lane={c.lane:14s} prio={c.priority_score:3d} "
                          f"manual={c.manual_rows} covered={c.existing_queryable_count} extracts={c.extract_rows} seed={1 if c.seed_ready else 0} page={c.page_kind} "
                          f"{c.family_name}")
                if discovery_mode:
                    print(f"[plan-only] discovery frontier detected: limiting real run to first {len(active_candidates)} candidate(s) per round")
                stopped_reason = "plan_only"
                break

            # 묶음 단위 처리
            consecutive_empty = 0
            if discovery_mode:
                print(f"[mode] discovery frontier: trying first {len(active_candidates)} candidate(s) with batch_size=1")
            round_inserted = process_round(
                conn, stage_db, benchmark_file, report_dir, active_candidates,
                baseline_bench, gemini, args, rounds_run, quarantined, run_items,
                total_inserted,
                discovery_mode=discovery_mode,
            )
            total_inserted += round_inserted
            print(f"[round {rounds_run}] inserted={round_inserted} "
                  f"total={total_inserted} quarantined={len(quarantined)}")

            if round_inserted == 0:
                consecutive_zero_progress += 1
                if discovery_mode and consecutive_zero_progress >= args.zero_progress_stop_rounds:
                    stopped_reason = "zero_insert_round_discovery"
                    break
                if consecutive_zero_progress >= args.zero_progress_stop_rounds and not discovery_mode:
                    stopped_reason = "zero_progress_rounds"
                    break
            else:
                consecutive_zero_progress = 0

            # snapshot (LRU)
            if args.snapshot_every > 0 and total_inserted > 0:
                last_snap_count = total_inserted - round_inserted
                # trigger snapshots when we cross multiples of snapshot_every
                while last_snap_count // args.snapshot_every < total_inserted // args.snapshot_every:
                    last_snap_count = ((last_snap_count // args.snapshot_every) + 1) * args.snapshot_every
                    snap_path = report_dir / f"snapshot_after_{last_snap_count:04d}.db"
                    try:
                        shutil.copy2(stage_db, snap_path)
                    except OSError as e:
                        print(f"[warn] snapshot failed: {e}")
                        break
                lru_trim_snapshots(report_dir, args.max_snapshots)

        if rounds_run >= args.max_rounds:
            stopped_reason = "max_rounds_reached"

    finally:
        final_state = get_state(conn)
        conn.close()
        summary = {
            "run_id": run_id,
            "canonical_db": str(canonical_db),
            "stage_db": str(stage_db),
            "benchmark_file": str(benchmark_file),
            "start_state": start_state,
            "final_state": final_state,
            "rounds_run": rounds_run,
            "total_inserted": total_inserted,
            "quarantined": sorted(quarantined),
            "quarantined_count": len(quarantined),
            "stopped_reason": stopped_reason,
            "args": vars(args),
            "baseline_benchmark": baseline_bench,
            "diagnostic_baseline": diagnostic_baseline,
        }
        summary_path = report_dir / "run_summary.json"
        summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False),
                                encoding="utf-8")
        items_path = report_dir / "run_items.jsonl"
        with items_path.open("w", encoding="utf-8") as f:
            for it in run_items:
                f.write(json.dumps(it, ensure_ascii=False) + "\n")

        print("\n" + "=" * 70)
        print(f"[DONE] stopped_reason={stopped_reason}")
        print(f"[state] start:  {start_state}")
        print(f"[state] final:  {final_state}")
        print(f"[state] delta:  queryable +{final_state['queryable']-start_state['queryable']} "
              f"family_cov +{final_state['family_coverage']-start_state['family_coverage']} "
              f"extracts +{final_state['reaction_extracts']-start_state['reaction_extracts']}")
        print(f"[disk] report size: {dir_size_bytes(report_dir)/(1024**3):.2f} GB")
        print(f"[summary] {summary_path}")
        print("=" * 70)
    return 0


def process_round(conn, stage_db, benchmark_file, report_dir, candidates,
                  baseline_bench, gemini, args, rounds_run,
                  quarantined, run_items, global_inserted_so_far,
                  discovery_mode: bool = False) -> int:
    """Process candidates in batches with benchmark regression check."""
    round_inserted = 0
    batch: list[tuple[Candidate, InsertResult, str]] = []  # (cand, ins_result, lane)
    batch_size = 1 if discovery_mode else args.batch_size

    def commit_batch():
        nonlocal round_inserted
        if not batch:
            return True, ""
        # Already inserted in stage DB (committed). Now run benchmark.
        suffix = f"round{rounds_run:02d}_batch{global_inserted_so_far+round_inserted:04d}"
        bench = run_benchmark(stage_db, benchmark_file, report_dir, suffix)
        regressed, why = has_regression(
            baseline_bench, bench,
            allowed_top1_drop=args.allowed_top1_drop,
            allowed_violation_increase=args.allowed_violation_increase,
        )
        if regressed:
            # Rollback this entire batch via DELETE
            print(f"  [batch] regression: {why} — rolling back {len(batch)} inserts")
            conn.execute("BEGIN")
            try:
                for (cand, ins, lane) in batch:
                    rollback_insert(conn, ins)
                    quarantined.add(cand.family_name_norm)
                    run_items.append({
                        "round": rounds_run, "family": cand.family_name,
                        "lane": lane, "status": "batch_rolled_back",
                        "reason": why,
                    })
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"  [batch] rollback failed: {e}")
            # Retry each one individually with Gemini fallback
            recovered = 0
            if args.gemini_fallback == "yes" and gemini is not None:
                print(f"  [batch] gemini fallback: retrying {len(batch)} individually")
                for (cand, _ins, _lane) in batch:
                    if cand.family_name_norm in quarantined:
                        # try to unquarantine for individual retry
                        quarantined.discard(cand.family_name_norm)
                    if retry_with_gemini(conn, stage_db, benchmark_file, report_dir,
                                         cand, baseline_bench, gemini, args,
                                         rounds_run, quarantined, run_items,
                                         global_inserted_so_far + round_inserted + recovered):
                        recovered += 1
                    else:
                        quarantined.add(cand.family_name_norm)
            batch.clear()
            return True, f"batch_regressed_rollback, recovered={recovered}"
        else:
            # accept batch
            for (cand, ins, lane) in batch:
                run_items.append({
                    "round": rounds_run, "family": cand.family_name,
                    "lane": lane, "status": "inserted",
                    "re_id": ins.reaction_extract_id,
                    "mol_ids": ins.molecule_ids,
                })
            round_inserted += len(batch)
            batch.clear()
            return True, "ok"

    for cand in candidates:
        if cand.family_name_norm in quarantined:
            continue

        # Lane decision
        use_gemini = False
        if args.gemini_only:
            use_gemini = True
        elif args.deterministic_only:
            use_gemini = False
        else:
            use_gemini = (cand.lane == "gemini")

        lane_used = "gemini" if use_gemini else "deterministic"
        ins: InsertResult | None = None

        try:
            if use_gemini:
                if gemini is None:
                    # no gemini available, skip or fallback to deterministic
                    if cand.lane == "deterministic":
                        ins = attempt_deterministic(conn, cand, gemini, args)
                        lane_used = "deterministic_fallback_no_gemini"
                    else:
                        quarantined.add(cand.family_name_norm)
                        run_items.append({
                            "round": rounds_run, "family": cand.family_name,
                            "lane": "skipped", "status": "no_gemini_available",
                        })
                        continue
                else:
                    ins = attempt_gemini(conn, cand, gemini, args)
            else:
                # deterministic first (with relaxed Gemini)
                ins = attempt_deterministic(conn, cand, gemini, args)
                if (ins is None or not ins.success) and gemini is not None \
                        and args.gemini_fallback == "yes":
                    # fallback to strict gemini
                    print(f"  [cand] deterministic failed for {cand.family_name}, "
                          f"falling back to strict Gemini")
                    ins = attempt_gemini(conn, cand, gemini, args)
                    lane_used = "gemini_fallback"
        except Exception as e:
            print(f"  [cand] exception on {cand.family_name}: {e}")
            traceback.print_exc()
            ins = None

        if ins is None or not ins.success:
            quarantined.add(cand.family_name_norm)
            run_items.append({
                "round": rounds_run, "family": cand.family_name,
                "lane": lane_used, "status": "insert_failed",
                "error": ins.error if ins else "no_result",
            })
            continue

        # Success — add to batch
        batch.append((cand, ins, lane_used))
        if len(batch) >= batch_size:
            ok, why = commit_batch()
            if not ok:
                break

    # flush remaining
    if batch:
        commit_batch()

    return round_inserted




def attempt_deterministic(conn, cand: Candidate, gemini: GeminiClient | None = None,
                          args=None) -> InsertResult | None:
    """
    True deterministic lane.
    Reuse an existing validated family extract already present in the DB and
    promote it into a canonical_overview seed without calling Gemini.
    """
    if cand.seed_extract_id is None:
        return InsertResult(success=False, error="deterministic lane has no reusable seed extract")

    payload = build_payload_from_existing_extract(conn, cand.seed_extract_id, cand.family_name)
    if payload is None:
        return InsertResult(success=False, error=f"failed to build deterministic payload from extract {cand.seed_extract_id}")

    conn.execute("BEGIN")
    try:
        ins = insert_family_seed(conn, cand, payload,
                                 structure_source="deterministic_seed_from_existing",
                                 model_name=f"extract:{cand.seed_extract_id}")
        if not ins.success:
            conn.rollback()
            return ins
        conn.commit()
        return ins
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return InsertResult(success=False, error=f"insert_exception: {e}")


def attempt_gemini(conn, cand: Candidate, gemini: GeminiClient, args) -> InsertResult | None:
    """Call Gemini, sanitize, insert. Auto-commits on success."""
    payload, model, err = call_gemini_for_candidate(gemini, cand)
    if err or payload is None:
        return InsertResult(success=False, error=f"gemini: {err}")
    if not isinstance(payload, dict):
        return InsertResult(success=False, error=f"gemini: invalid normalized payload type {type(payload).__name__}")
    decision = str(payload.get("decision", "")).strip().lower()
    conf = float(payload.get("confidence", 0.0) or 0.0)
    if decision != "promote":
        return InsertResult(success=False, error=f"gemini: {decision} (conf={conf})")
    if conf < args.min_confidence:
        return InsertResult(
            success=False,
            error=f"gemini: confidence too low ({conf} < {args.min_confidence})",
        )
    if payload.get("is_generic_risky") is True:
        return InsertResult(success=False, error="gemini: is_generic_risky=true")

    # INSERT under transaction
    conn.execute("BEGIN")
    try:
        ins = insert_family_seed(conn, cand, payload,
                                 structure_source="gemini_auto_seed",
                                 model_name=model)
        if not ins.success:
            conn.rollback()
            return ins
        conn.commit()
        return ins
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return InsertResult(success=False, error=f"insert_exception: {e}")


def retry_with_gemini(conn, stage_db, benchmark_file, report_dir, cand,
                     baseline_bench, gemini, args, rounds_run,
                     quarantined, run_items, counter) -> bool:
    """Retry a single candidate via Gemini with benchmark check."""
    ins = attempt_gemini(conn, cand, gemini, args)
    if ins is None or not ins.success:
        run_items.append({
            "round": rounds_run, "family": cand.family_name,
            "lane": "gemini_retry", "status": "insert_failed",
            "error": ins.error if ins else "no_result",
        })
        return False

    suffix = f"round{rounds_run:02d}_retry{counter:04d}"
    bench = run_benchmark(stage_db, benchmark_file, report_dir, suffix)
    regressed, why = has_regression(
        baseline_bench, bench,
        allowed_top1_drop=args.allowed_top1_drop,
        allowed_violation_increase=args.allowed_violation_increase,
    )
    if regressed:
        conn.execute("BEGIN")
        try:
            rollback_insert(conn, ins)
            conn.commit()
        except Exception:
            conn.rollback()
        run_items.append({
            "round": rounds_run, "family": cand.family_name,
            "lane": "gemini_retry", "status": "rolled_back",
            "reason": why,
        })
        return False
    run_items.append({
        "round": rounds_run, "family": cand.family_name,
        "lane": "gemini_retry", "status": "inserted",
        "re_id": ins.reaction_extract_id,
        "mol_ids": ins.molecule_ids,
    })
    return True


if __name__ == "__main__":
    sys.exit(main())
