from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import difflib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import requests
from dotenv import load_dotenv

try:
    from rdkit import Chem  # type: ignore
except Exception:
    Chem = None

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "app" / "labint.db"
DEFAULT_STAGE_DB = ROOT / "app" / "labint_gemini_autorun_exp.db"
DEFAULT_REPORT_ROOT = ROOT / "reports" / "gemini_family_automation"
BENCHMARK_SCRIPT = ROOT / "run_named_reaction_benchmark_small.py"
VERIFY_SCRIPT = ROOT / "VERIFY_CURRENT_BACKEND_STATE.py"
TAG = "gemini_auto_family_seed_v1"
NOW_FMT = "%Y-%m-%d %H:%M:%S"
TS_FMT = "%Y%m%d_%H%M%S"

GENERIC_RISKY_FAMILIES = {
    "aldol reaction",
    "barbier coupling reaction",
    "alder ene reaction hydro allyl addition",
    "michael addition",
    "michael reaction",
    "cope rearrangement",
    "claisen condensation claisen reaction",
    "diels alder cycloaddition",
}

SKIP_FAMILIES = {
    "appendix",
}

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}


@dataclass
class CandidatePacket:
    family_name: str
    family_name_norm: str
    page_no: int | None
    page_kind: str | None
    title: str | None
    summary: str | None
    notes: str | None
    image_filename: str | None
    source_label: str | None
    source_doc: str | None
    entities: list[dict[str, Any]]
    similar_existing: list[dict[str, Any]]
    exact_existing_family: str | None
    exact_existing_pattern_family: str | None
    manual_rows: int
    extract_rows: int
    exact_like_score: int
    priority_score: int


def now_ts() -> str:
    return dt.datetime.now().strftime(TS_FMT)


def now_human() -> str:
    return dt.datetime.now().strftime(NOW_FMT)


def norm_text(s: str | None) -> str:
    s = unicodedata.normalize("NFKD", s or "").lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def safe_name(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s).strip("_") or "run"


def yesno(v: str | bool) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_family_csv(s: str | None) -> set[str]:
    out: set[str] = set()
    for part in str(s or "").split(","):
        n = norm_text(part)
        if n:
            out.add(n)
    return out


def require_rdkit() -> None:
    if Chem is None:
        raise RuntimeError("RDKit import failed. Run this inside the chemlens conda env.")


def canon_smiles(smiles: str) -> str:
    require_rdkit()
    mol = Chem.MolFromSmiles((smiles or "").strip())
    if mol is None:
        raise ValueError(f"RDKit failed to parse SMILES: {smiles}")
    return Chem.MolToSmiles(mol, canonical=True)


def load_env(root: Path) -> None:
    env_path = root / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)


class GeminiClient:
    def __init__(self, api_key: str, preferred_models: list[str], timeout: int = 90):
        self.api_key = api_key.strip()
        self.preferred_models = [m.strip() for m in preferred_models if m and m.strip()]
        self.timeout = timeout
        self._available_models: list[str] | None = None

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        r = requests.request(method, url, timeout=self.timeout, **kwargs)
        return r

    def list_models(self) -> list[str]:
        if self._available_models is not None:
            return self._available_models
        if not self.api_key:
            self._available_models = []
            return []
        url = f"https://generativelanguage.googleapis.com/v1beta/models?key={self.api_key}"
        try:
            r = self._request("GET", url)
            if r.status_code >= 400:
                self._available_models = []
                return []
            data = r.json()
            out: list[str] = []
            for item in data.get("models", []):
                name = str(item.get("name") or "")
                if name.startswith("models/"):
                    name = name.split("/", 1)[1]
                methods = set(item.get("supportedGenerationMethods") or [])
                if not methods or "generateContent" in methods:
                    out.append(name)
            self._available_models = out
            return out
        except Exception:
            self._available_models = []
            return []

    def resolve_models(self) -> list[str]:
        available = set(self.list_models())
        if available:
            preferred = [m for m in self.preferred_models if m in available]
            if preferred:
                return preferred
            # If no preferred match, fall back to current widely used text/vision-capable names if present.
            soft_fallback = [
                "gemini-2.5-pro",
                "gemini-2.5-flash",
                "gemini-2.5-flash-lite",
                "gemini-3-pro",
                "gemini-3-flash",
            ]
            resolved = [m for m in soft_fallback if m in available]
            if resolved:
                return resolved
            return sorted(available)
        return self.preferred_models

    def generate_json(
        self,
        prompt: str,
        system_instruction: str,
        image_path: Path | None = None,
        max_retries: int = 2,
    ) -> tuple[dict[str, Any], str]:
        if not self.api_key:
            raise RuntimeError("GEMINI_API_KEY is not set")
        last_err: str | None = None
        for model in self.resolve_models():
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={self.api_key}"
            parts: list[dict[str, Any]] = []
            if image_path and image_path.exists() and image_path.suffix.lower() in IMAGE_EXTS:
                mime = {
                    ".jpg": "image/jpeg",
                    ".jpeg": "image/jpeg",
                    ".png": "image/png",
                    ".webp": "image/webp",
                }[image_path.suffix.lower()]
                b64 = base64.b64encode(image_path.read_bytes()).decode("ascii")
                parts.append({"inline_data": {"mime_type": mime, "data": b64}})
            parts.append({"text": prompt})
            payload = {
                "contents": [{"parts": parts}],
                "systemInstruction": {"parts": [{"text": system_instruction}]},
                "generationConfig": {
                    "responseMimeType": "application/json",
                    "temperature": 0.2,
                },
            }
            for attempt in range(1, max_retries + 1):
                try:
                    r = self._request("POST", url, json=payload)
                    if r.status_code == 404:
                        last_err = f"{model} returned 404"
                        break
                    if r.status_code >= 400:
                        last_err = f"{model} HTTP {r.status_code}: {r.text[:500]}"
                        time.sleep(1.0 * attempt)
                        continue
                    data = r.json()
                    text = extract_text_from_gemini(data)
                    return parse_json_text(text), model
                except Exception as e:
                    last_err = f"{model} attempt {attempt}: {e}"
                    time.sleep(1.0 * attempt)
                    continue
        raise RuntimeError(last_err or "Gemini request failed")


def extract_text_from_gemini(data: dict[str, Any]) -> str:
    cands = data.get("candidates") or []
    if not cands:
        raise RuntimeError(f"Gemini empty response: {json.dumps(data, ensure_ascii=False)[:600]}")
    parts = (((cands[0] or {}).get("content") or {}).get("parts") or [])
    texts: list[str] = []
    for part in parts:
        if isinstance(part, dict) and part.get("text"):
            texts.append(part["text"])
    text = "\n".join(texts).strip()
    if not text:
        raise RuntimeError(f"Gemini returned no text parts: {json.dumps(data, ensure_ascii=False)[:600]}")
    return text


def parse_json_text(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        raw = raw.strip()
    try:
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            raise ValueError("JSON root is not an object")
        return obj
    except Exception:
        m = re.search(r"\{.*\}", raw, flags=re.S)
        if not m:
            raise
        obj = json.loads(m.group(0))
        if not isinstance(obj, dict):
            raise ValueError("JSON root is not an object")
        return obj


def ensure_stage_db(canonical_db: Path, stage_db: Path, reset: bool) -> None:
    stage_db.parent.mkdir(parents=True, exist_ok=True)
    if reset and stage_db.exists():
        stage_db.unlink()
    if not stage_db.exists():
        shutil.copy2(canonical_db, stage_db)


def scalar(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> Any:
    row = conn.execute(sql, tuple(params)).fetchone()
    return row[0] if row else None


def current_state(conn: sqlite3.Connection) -> dict[str, Any]:
    return {
        "queryable": scalar(conn, "select count(*) from extract_molecules where queryable=1"),
        "family_coverage": scalar(conn, "select count(distinct reaction_family_name) from extract_molecules where queryable=1"),
        "reaction_extracts": scalar(conn, "select count(*) from reaction_extracts"),
        "family_patterns": scalar(conn, "select count(distinct family_name) from reaction_family_patterns"),
    }


def get_existing_family_maps(conn: sqlite3.Connection) -> tuple[dict[str, str], dict[str, str]]:
    covered = {}
    for fam, in conn.execute("select distinct trim(reaction_family_name) from extract_molecules where queryable=1 and trim(coalesce(reaction_family_name,''))<>''"):
        covered[norm_text(fam)] = fam
    patterns = {}
    for fam, in conn.execute("select distinct trim(family_name) from reaction_family_patterns where trim(coalesce(family_name,''))<>''"):
        patterns[norm_text(fam)] = fam
    return covered, patterns


def gather_similar_families(conn: sqlite3.Connection, family_name: str, limit: int = 5) -> list[dict[str, Any]]:
    all_names = [r[0] for r in conn.execute("select distinct family_name from reaction_family_patterns where trim(coalesce(family_name,''))<>''")]
    family_norm = norm_text(family_name)
    scored: list[tuple[float, str]] = []
    for fam in all_names:
        score = difflib.SequenceMatcher(None, family_norm, norm_text(fam)).ratio()
        if score >= 0.55:
            scored.append((score, fam))
    scored.sort(key=lambda x: (-x[0], x[1]))
    out = []
    for score, fam in scored[:limit]:
        row = conn.execute(
            """
            select family_name, family_class, transformation_type, key_reagents_clue, description_short
            from reaction_family_patterns where family_name=? limit 1
            """,
            (fam,),
        ).fetchone()
        out.append({
            "family_name": row[0],
            "score": round(score, 3),
            "family_class": row[1],
            "transformation_type": row[2],
            "key_reagents_clue": row[3],
            "description_short": row[4],
        })
    return out


def looks_like_exact_candidate(text: str | None) -> bool:
    t = (text or "").strip()
    if not t or ";" in t or "," in t or len(t) > 80:
        return False
    if re.search(r"\b(reactant|product|intermediate|compound|derivative|adduct|solvent|catalyst|reagent|acid or base|steps|temperature|heat|hv|mixture|salt)\b", t, re.I):
        return False
    if re.search(r"\b(R\d*|Ar|Het|EWG|PG|LG|X|Y|Z)\b|\[\*", t):
        return False
    words = t.split()
    return 1 <= len(words) <= 5


def choose_anchor_page(conn: sqlite3.Connection, family_name: str) -> sqlite3.Row | None:
    row = conn.execute(
        """
        select *
        from manual_page_knowledge
        where trim(reference_family_name)=?
        order by case
            when lower(page_kind) in ('overview', 'importance', 'mechanism', 'importance_and_mechanism') then 0
            when lower(page_kind) like '%overview%' then 1
            else 2
        end,
        page_no asc, id asc
        limit 1
        """,
        (family_name,),
    ).fetchone()
    if row:
        return row
    row = conn.execute(
        """
        select * from manual_page_knowledge
        where coalesce(family_names,'') like ?
        order by page_no asc, id asc
        limit 1
        """,
        (f"%{family_name}%",),
    ).fetchone()
    return row


def select_candidates(
    conn: sqlite3.Connection,
    candidate_limit: int,
    allow_generic: bool,
    include_families: set[str] | None = None,
    exclude_families: set[str] | None = None,
    runtime_exclude_families: set[str] | None = None,
) -> list[CandidatePacket]:
    covered_map, pattern_map = get_existing_family_maps(conn)
    manual_families = [
        r[0]
        for r in conn.execute(
            "select distinct trim(reference_family_name) from manual_page_knowledge where trim(coalesce(reference_family_name,''))<>''"
        )
    ]
    packets: list[CandidatePacket] = []
    for fam in manual_families:
        fam_norm = norm_text(fam)
        if not fam_norm or fam_norm in SKIP_FAMILIES:
            continue
        if include_families and fam_norm not in include_families:
            continue
        if exclude_families and fam_norm in exclude_families:
            continue
        if runtime_exclude_families and fam_norm in runtime_exclude_families:
            continue
        if fam_norm in covered_map:
            continue
        if (not allow_generic) and fam_norm in GENERIC_RISKY_FAMILIES:
            continue
        page = choose_anchor_page(conn, fam)
        if not page:
            continue
        page_id = page["id"]
        ents = [
            {
                "entity_text": r[0],
                "entity_type": r[1],
                "notes": r[2],
                "confidence": r[3],
            }
            for r in conn.execute(
                """
                select entity_text, entity_type, notes, confidence
                from manual_page_entities
                where page_knowledge_id=?
                order by confidence desc, id asc
                limit 20
                """,
                (page_id,),
            )
        ]
        manual_rows = scalar(conn, "select count(*) from manual_page_knowledge where reference_family_name=?", (fam,)) or 0
        extract_rows = scalar(conn, "select count(*) from reaction_extracts where reaction_family_name=?", (fam,)) or 0
        exact_like_score = 0
        for txt, _role, cnt in conn.execute(
            """
            select em.normalized_text, em.role, count(*) c
            from extract_molecules em
            join reaction_extracts rx on rx.id=em.extract_id
            where rx.reaction_family_name=? and em.queryable=0 and em.quality_tier=3 and trim(coalesce(em.normalized_text,''))<>''
            group by em.normalized_text, em.role
            order by c desc
            """,
            (fam,),
        ):
            if looks_like_exact_candidate(txt):
                exact_like_score += int(cnt)
        priority_score = int(manual_rows) * 5 + int(extract_rows) * 3 + min(int(exact_like_score), 10) * 2
        packets.append(
            CandidatePacket(
                family_name=fam,
                family_name_norm=fam_norm,
                page_no=page["page_no"],
                page_kind=page["page_kind"],
                title=page["title"],
                summary=page["summary"],
                notes=page["notes"],
                image_filename=page["image_filename"],
                source_label=page["source_label"],
                source_doc=None,
                entities=ents,
                similar_existing=gather_similar_families(conn, fam, limit=5),
                exact_existing_family=covered_map.get(fam_norm),
                exact_existing_pattern_family=pattern_map.get(fam_norm),
                manual_rows=int(manual_rows),
                extract_rows=int(extract_rows),
                exact_like_score=int(exact_like_score),
                priority_score=priority_score,
            )
        )
    packets.sort(key=lambda p: (-p.priority_score, -(p.exact_like_score), -(p.extract_rows), p.family_name))
    return packets[:candidate_limit]


def resolve_image_path(root: Path, image_filename: str | None) -> Path | None:
    if not image_filename:
        return None
    direct = root / "app" / "data" / "images" / image_filename
    if direct.exists():
        return direct
    for p in (root / "app").rglob(image_filename):
        if p.is_file() and p.suffix.lower() in IMAGE_EXTS:
            return p
    return None


def make_prompt(packet: CandidatePacket, allow_create_new: bool, allow_generic: bool) -> tuple[str, str]:
    system = (
        "You are a medicinal chemistry and organic reaction indexing assistant for a local CHEMLENS database. "
        "Your job is to decide whether a candidate manual-page family should merge into an existing family, create a new family, be held, or be rejected. "
        "Return strict JSON only. Use chemically plausible representative example substrates/products, not placeholders. "
        "Prefer simple canonical textbook-like examples. Never invent impossible SMILES."
    )
    payload = {
        "candidate_family_name": packet.family_name,
        "page_no": packet.page_no,
        "page_kind": packet.page_kind,
        "title": packet.title,
        "summary": packet.summary,
        "notes": packet.notes,
        "image_filename": packet.image_filename,
        "manual_rows": packet.manual_rows,
        "extract_rows": packet.extract_rows,
        "exact_like_score": packet.exact_like_score,
        "similar_existing": packet.similar_existing,
        "allow_create_new": allow_create_new,
        "allow_generic": allow_generic,
        "entities": packet.entities,
    }
    required_schema = {
        "decision": "merge_existing | create_new | hold | reject",
        "confidence": "0.0-1.0",
        "family_name": "target family name to use if created or kept",
        "matched_existing_family": "existing family name if decision=merge_existing else null",
        "family_class": "string or null",
        "transformation_type": "string or null",
        "mechanism_type": "string or null",
        "key_reagents_clue": "string or null",
        "common_conditions": "string or null",
        "description_short": "one concise sentence",
        "extract_kind": "canonical_overview | application_example | mechanism_example | variant_example",
        "transformation_text": "one concise sentence",
        "reactants_text": "pipe-separated text",
        "products_text": "pipe-separated text",
        "reagents_text": "pipe-separated text or empty string",
        "conditions_text": "concise text",
        "notes_text": "brief rationale",
        "molecules": [
            {
                "role": "reactant | product | reagent | catalyst | intermediate",
                "smiles": "valid SMILES",
                "name": "short label",
                "source_field": "reactants_text | products_text | reagents_text | conditions_text | notes_text",
            }
        ],
    }
    prompt = (
        "Use the candidate packet below. Decide whether this should merge into an existing family or create a new one.\n"
        "Prefer merge_existing when the chemistry is clearly the same family under a naming variant.\n"
        "Use create_new only when the family is truly distinct and recognizable.\n"
        "Return JSON matching the schema.\n\n"
        f"Candidate packet:\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n\n"
        f"Required JSON schema guide:\n{json.dumps(required_schema, ensure_ascii=False, indent=2)}\n\n"
        "Hard rules:\n"
        "- Use at least 1 reactant and 1 product.\n"
        "- Use 2 to 5 molecules total when possible.\n"
        "- No placeholders like R, Ar, Het, EWG, substrate A.\n"
        "- Do not wrap JSON in markdown fences.\n"
        "- If the family is too generic or unsafe to auto-seed, return hold or reject.\n"
        "- If merging, matched_existing_family must be one of the provided similar_existing names when appropriate.\n"
    )
    return system, prompt


def validate_gemini_payload(payload: dict[str, Any], packet: CandidatePacket, allow_generic: bool, min_confidence: float) -> tuple[bool, list[str]]:
    errors: list[str] = []
    decision = str(payload.get("decision") or "").strip().lower()
    if decision not in {"merge_existing", "create_new", "hold", "reject"}:
        errors.append("invalid decision")
    try:
        conf = float(payload.get("confidence") or 0.0)
    except Exception:
        conf = 0.0
        errors.append("invalid confidence")
    if decision in {"merge_existing", "create_new"} and conf < min_confidence:
        errors.append(f"confidence below threshold: {conf} < {min_confidence}")
    family_name = str(payload.get("family_name") or packet.family_name).strip()
    if (not allow_generic) and norm_text(family_name) in GENERIC_RISKY_FAMILIES:
        errors.append("generic risky family blocked by settings")
    if decision == "merge_existing" and not str(payload.get("matched_existing_family") or "").strip():
        errors.append("merge_existing requires matched_existing_family")
    if decision in {"merge_existing", "create_new"}:
        molecules = payload.get("molecules") or []
        if not isinstance(molecules, list) or not molecules:
            errors.append("molecules missing")
        else:
            has_reactant = False
            has_product = False
            for idx, mol in enumerate(molecules):
                if not isinstance(mol, dict):
                    errors.append(f"molecule[{idx}] not an object")
                    continue
                role = str(mol.get("role") or "").strip().lower()
                if role == "reactant":
                    has_reactant = True
                if role == "product":
                    has_product = True
                smiles = str(mol.get("smiles") or "").strip()
                if not smiles:
                    errors.append(f"molecule[{idx}] empty smiles")
                    continue
                try:
                    canon_smiles(smiles)
                except Exception as e:
                    errors.append(f"molecule[{idx}] invalid smiles: {e}")
            if not has_reactant:
                errors.append("no reactant molecule")
            if not has_product:
                errors.append("no product molecule")
    return not errors, errors


def find_anchor_scheme_candidate(conn: sqlite3.Connection, packet: CandidatePacket, target_family_name: str) -> tuple[int, int | None, str | None]:
    row = None
    if packet.image_filename:
        row = conn.execute(
            """
            select sc.id, pi.page_no, pi.image_filename
            from scheme_candidates sc
            join page_images pi on pi.id = sc.page_image_id
            where pi.image_filename=?
            order by case when lower(coalesce(sc.scheme_role,''))='canonical_overview' then 0 else 1 end,
                     sc.id asc
            limit 1
            """,
            (packet.image_filename,),
        ).fetchone()
    if row is None and packet.page_no is not None:
        row = conn.execute(
            """
            select sc.id, pi.page_no, pi.image_filename
            from scheme_candidates sc
            join page_images pi on pi.id = sc.page_image_id
            where pi.page_no=?
            order by sc.id asc
            limit 1
            """,
            (packet.page_no,),
        ).fetchone()
    if row is None:
        row = conn.execute(
            """
            select sc.id, pi.page_no, pi.image_filename
            from scheme_candidates sc
            join page_images pi on pi.id = sc.page_image_id
            join reaction_extracts re on re.scheme_candidate_id=sc.id
            where trim(re.reaction_family_name)=?
            order by sc.id asc
            limit 1
            """,
            (target_family_name,),
        ).fetchone()
    if row is None:
        row = conn.execute(
            "select sc.id, pi.page_no, pi.image_filename from scheme_candidates sc join page_images pi on pi.id=sc.page_image_id order by sc.id asc limit 1"
        ).fetchone()
    if row is None:
        raise RuntimeError(f"No anchor scheme candidate available for {target_family_name}")
    return int(row[0]), row[1], row[2]


def maybe_upsert_family_pattern(conn: sqlite3.Connection, family_name: str, payload: dict[str, Any], seeded_from: str, now: str) -> tuple[bool, str]:
    fam_norm = norm_text(family_name)
    row = conn.execute(
        "select id, family_name from reaction_family_patterns where family_name_norm=? limit 1",
        (fam_norm,),
    ).fetchone()
    if row:
        conn.execute(
            """
            update reaction_family_patterns
            set family_class=coalesce(family_class, ?),
                transformation_type=coalesce(transformation_type, ?),
                mechanism_type=coalesce(mechanism_type, ?),
                key_reagents_clue=coalesce(key_reagents_clue, ?),
                common_conditions=coalesce(common_conditions, ?),
                description_short=coalesce(description_short, ?),
                latest_updated_at=?,
                updated_at=?
            where id=?
            """,
            (
                payload.get("family_class"),
                payload.get("transformation_type"),
                payload.get("mechanism_type"),
                payload.get("key_reagents_clue"),
                payload.get("common_conditions"),
                payload.get("description_short"),
                now,
                now,
                row[0],
            ),
        )
        return False, row[1]
    conn.execute(
        """
        insert into reaction_family_patterns (
            family_name, family_name_norm, family_class, transformation_type, mechanism_type,
            key_reagents_clue, common_conditions, description_short, latest_updated_at,
            latest_source_zip, seeded_from, created_at, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            family_name,
            fam_norm,
            payload.get("family_class"),
            payload.get("transformation_type"),
            payload.get("mechanism_type"),
            payload.get("key_reagents_clue"),
            payload.get("common_conditions"),
            payload.get("description_short"),
            now,
            packet_source_zip(packet=None),
            seeded_from,
            now,
            now,
        ),
    )
    return True, family_name


def packet_source_zip(packet: CandidatePacket | None) -> str:
    return packet.source_label if packet and packet.source_label else "gemini_family_automation"


def existing_seed_extract(conn: sqlite3.Connection, target_family_name: str) -> int | None:
    row = conn.execute(
        "select id from reaction_extracts where reaction_family_name=? and extractor_prompt_version=? limit 1",
        (target_family_name, TAG),
    ).fetchone()
    return int(row[0]) if row else None


def insert_seed(conn: sqlite3.Connection, packet: CandidatePacket, payload: dict[str, Any], model_name: str) -> dict[str, Any]:
    now = now_human()
    decision = str(payload.get("decision") or "").strip().lower()
    target_family_name = str(payload.get("family_name") or packet.family_name).strip() or packet.family_name
    if decision == "merge_existing":
        target_family_name = str(payload.get("matched_existing_family") or target_family_name).strip() or target_family_name
    if existing_seed_extract(conn, target_family_name):
        return {
            "family_name": packet.family_name,
            "target_family_name": target_family_name,
            "status": "skipped_existing_seed",
            "decision": decision,
            "model_name": model_name,
        }

    created_pattern, canonical_family_name = maybe_upsert_family_pattern(
        conn,
        target_family_name,
        payload,
        seeded_from="gemini_auto_create_family" if decision == "create_new" else "gemini_auto_merge_family",
        now=now,
    )

    sc_id, page_no, image_filename = find_anchor_scheme_candidate(conn, packet, canonical_family_name)
    notes_text = str(payload.get("notes_text") or "").strip()
    notes_text = (notes_text + f" [{TAG}] candidate={packet.family_name}").strip()
    cur = conn.execute(
        """
        insert into reaction_extracts (
            scheme_candidate_id, reaction_family_name, reaction_family_name_norm, extract_kind,
            transformation_text, reactants_text, products_text, reagents_text,
            conditions_text, notes_text, reactant_smiles, product_smiles,
            smiles_confidence, extraction_confidence, parse_status, promote_decision,
            extractor_model, extractor_prompt_version, extraction_raw_json, created_at, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            sc_id,
            canonical_family_name,
            norm_text(canonical_family_name),
            str(payload.get("extract_kind") or "canonical_overview"),
            str(payload.get("transformation_text") or "").strip(),
            str(payload.get("reactants_text") or "").strip(),
            str(payload.get("products_text") or "").strip(),
            str(payload.get("reagents_text") or "").strip(),
            str(payload.get("conditions_text") or "").strip(),
            notes_text,
            None,
            None,
            float(payload.get("confidence") or 0.85),
            float(payload.get("confidence") or 0.85),
            "gemini_auto_curated",
            "promote",
            model_name,
            TAG,
            json.dumps(payload, ensure_ascii=False),
            now,
            now,
        ),
    )
    extract_id = int(cur.lastrowid)

    reactants: list[str] = []
    products: list[str] = []
    inserted = 0
    invalid_smiles: list[str] = []
    for mol in payload.get("molecules") or []:
        role = str(mol.get("role") or "").strip().lower()
        raw_smiles = str(mol.get("smiles") or "").strip()
        try:
            smiles = canon_smiles(raw_smiles)
        except Exception:
            invalid_smiles.append(raw_smiles)
            continue
        name = str(mol.get("name") or "").strip() or smiles
        source_field = str(mol.get("source_field") or "").strip() or (
            "reactants_text" if role == "reactant" else "products_text" if role == "product" else "reagents_text"
        )
        conn.execute(
            """
            insert into extract_molecules (
                extract_id, role, smiles, smiles_kind, quality_tier, reaction_family_name,
                source_zip, page_no, queryable, note_text, normalized_text, source_field,
                structure_source, role_confidence, created_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                extract_id,
                role,
                smiles,
                "smiles",
                1,
                canonical_family_name,
                packet_source_zip(packet),
                page_no,
                1,
                f"{TAG} | {name} | anchor={image_filename or packet.image_filename or ''} | decision={decision}",
                name,
                source_field,
                "gemini_auto_seed",
                float(payload.get("confidence") or 0.85),
                now,
            ),
        )
        inserted += 1
        if role == "reactant":
            reactants.append(smiles)
        elif role == "product":
            products.append(smiles)

    if inserted == 0 or not reactants or not products:
        conn.execute("delete from extract_molecules where extract_id=?", (extract_id,))
        conn.execute("delete from reaction_extracts where id=?", (extract_id,))
        if created_pattern:
            conn.execute("delete from reaction_family_patterns where family_name_norm=? and seeded_from like 'gemini_auto_%'", (norm_text(canonical_family_name),))
        return {
            "family_name": packet.family_name,
            "target_family_name": canonical_family_name,
            "status": "skipped_invalid_smiles",
            "decision": decision,
            "model_name": model_name,
            "created_pattern": False,
            "invalid_smiles": " | ".join([s for s in invalid_smiles if s][:10]),
        }

    conn.execute(
        "update reaction_extracts set reactant_smiles=?, product_smiles=? where id=?",
        (
            " | ".join(reactants) if reactants else None,
            " | ".join(products) if products else None,
            extract_id,
        ),
    )

    return {
        "family_name": packet.family_name,
        "target_family_name": canonical_family_name,
        "status": "inserted",
        "decision": decision,
        "model_name": model_name,
        "created_pattern": created_pattern,
        "extract_id": extract_id,
        "inserted_molecules": inserted,
        "page_no": page_no,
        "image_filename": image_filename,
    }


def run_benchmark(db_path: Path, report_dir: Path, suffix: str) -> dict[str, Any]:
    json_out = report_dir / f"benchmark_{suffix}.json"
    csv_out = report_dir / f"benchmark_{suffix}.csv"
    md_out = report_dir / f"benchmark_{suffix}.md"
    cmd = [
        sys.executable,
        str(BENCHMARK_SCRIPT),
        "--db",
        str(db_path),
        "--json-out",
        str(json_out),
        "--csv-out",
        str(csv_out),
        "--report-md",
        str(md_out),
    ]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"benchmark failed: {proc.stderr or proc.stdout}")
    data = json.loads(json_out.read_text(encoding="utf-8"))
    return {
        "summary": data.get("summary") or {},
        "json_out": str(json_out),
        "csv_out": str(csv_out),
        "md_out": str(md_out),
        "stdout": proc.stdout,
    }


def verify_db_state(db_path: Path) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        p502 = cur.execute("select title from manual_page_knowledge where page_no=502 limit 1").fetchone()
        return {
            "db": str(db_path),
            "manual_page_knowledge": scalar(conn, "select count(*) from manual_page_knowledge"),
            "manual_page_entities": scalar(conn, "select count(*) from manual_page_entities"),
            "distinct_pages": scalar(conn, "select count(distinct page_no) from manual_page_knowledge"),
            "page_range": cur.execute("select min(page_no), max(page_no) from manual_page_knowledge").fetchone(),
            "p502_553_records": scalar(conn, "select count(*) from manual_page_knowledge where page_no between 502 and 553"),
            "p502_title": p502[0] if p502 else None,
            "reaction_family_patterns": scalar(conn, "select count(distinct family_name) from reaction_family_patterns"),
            "abbreviation_aliases": scalar(conn, "select count(*) from abbreviation_aliases"),
            "reaction_extracts": scalar(conn, "select count(*) from reaction_extracts"),
            "extract_molecules_total": scalar(conn, "select count(*) from extract_molecules"),
            "queryable": scalar(conn, "select count(*) from extract_molecules where queryable=1"),
            "tier1": scalar(conn, "select count(*) from extract_molecules where queryable=1 and quality_tier=1"),
            "tier2": scalar(conn, "select count(*) from extract_molecules where queryable=1 and quality_tier=2"),
            "tier3": scalar(conn, "select count(*) from extract_molecules where quality_tier=3"),
            "queryable_family_coverage": scalar(conn, "select count(distinct reaction_family_name) from extract_molecules where queryable=1"),
            "structure_source_counts": cur.execute("select coalesce(structure_source,'NULL'), count(*) from extract_molecules group by 1 order by 2 desc").fetchall(),
        }
    finally:
        conn.close()


def run_verify(db_path: Path, report_dir: Path, suffix: str) -> dict[str, Any]:
    out_path = report_dir / f"verify_{suffix}.json"
    data = verify_db_state(db_path)
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return data


def should_stop_for_regression(base: dict[str, Any], cur: dict[str, Any], allowed_top1_drop: float, allowed_violation_increase: int) -> tuple[bool, str]:
    base_top1 = float((base.get("summary") or {}).get("top1_accuracy") or 0.0)
    cur_top1 = float((cur.get("summary") or {}).get("top1_accuracy") or 0.0)
    base_viols = int((base.get("summary") or {}).get("disallow_top3_violations") or 0)
    cur_viols = int((cur.get("summary") or {}).get("disallow_top3_violations") or 0)
    if cur_top1 + allowed_top1_drop < base_top1:
        return True, f"top1 regression: baseline={base_top1} current={cur_top1} allowed_drop={allowed_top1_drop}"
    if cur_viols > base_viols + allowed_violation_increase:
        return True, f"disallow violations increased: baseline={base_viols} current={cur_viols}"
    return False, ""


def write_run_summary(report_dir: Path, summary: dict[str, Any], items: list[dict[str, Any]]) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "run_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    fieldnames: list[str] = []
    for item in items:
        for key in item.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with (report_dir / "run_items.csv").open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
            writer.writerows(items)
    lines = []
    lines.append("# Gemini family automation run")
    lines.append("")
    lines.append(f"- run_id: {summary.get('run_id')}")
    lines.append(f"- canonical_db: {summary.get('canonical_db')}")
    lines.append(f"- stage_db: {summary.get('stage_db')}")
    lines.append(f"- family_target: {summary.get('family_target')}")
    lines.append(f"- inserted_count: {summary.get('inserted_count')}")
    lines.append(f"- created_pattern_count: {summary.get('created_pattern_count')}")
    lines.append(f"- final_family_coverage: {summary.get('final_state', {}).get('family_coverage')}")
    lines.append("")
    lines.append("## Decisions")
    lines.append("")
    lines.append("| family | status | decision | target_family | model | created_pattern | note |")
    lines.append("|---|---|---|---|---|---:|---|")
    for item in items:
        lines.append(
            f"| {item.get('family_name','')} | {item.get('status','')} | {item.get('decision','')} | {item.get('target_family_name','')} | {item.get('model_name','')} | {item.get('created_pattern','')} | {item.get('note','')} |"
        )
    (report_dir / "run_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Gemini-driven family automation for CHEMLENS stage DB.")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Canonical baseline DB (read-only source)")
    parser.add_argument("--stage-db", default=str(DEFAULT_STAGE_DB), help="Experiment/staging DB path")
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_ROOT), help="Root directory for run reports")
    parser.add_argument("--candidate-limit", type=int, default=12, help="How many uncovered families to consider per round")
    parser.add_argument("--max-rounds", type=int, default=20)
    parser.add_argument("--family-target", type=int, default=80)
    parser.add_argument("--allow-generic", default="no", help="Allow risky generic families (yes/no)")
    parser.add_argument("--allow-create-new", default="yes", help="Allow creation of new family pattern rows (yes/no)")
    parser.add_argument("--min-confidence", type=float, default=0.72)
    parser.add_argument("--benchmark", default="yes", help="Run benchmark before and during automation (yes/no)")
    parser.add_argument("--benchmark-every", type=int, default=3)
    parser.add_argument("--stop-on-regression", default="yes", help="Stop if benchmark regresses beyond thresholds")
    parser.add_argument("--allowed-top1-drop", type=float, default=0.0)
    parser.add_argument("--allowed-violation-increase", type=int, default=0)
    parser.add_argument("--snapshot-every", type=int, default=5)
    parser.add_argument("--reset-stage", action="store_true", help="Re-copy stage DB from canonical before running")
    parser.add_argument("--plan-only", action="store_true", help="Select candidates and write plan, but do not call Gemini or mutate DB")
    parser.add_argument("--chaos-mode", default="no", help="Convenience flag: lower confidence gate and allow generic families")
    parser.add_argument("--include-families", default="", help="Comma-separated family names to allow (normalized exact match)")
    parser.add_argument("--exclude-families", default="", help="Comma-separated family names to force-exclude")
    parser.add_argument("--rollback-on-regression", default="yes", help="Rollback offending candidate and continue instead of stopping immediately")
    parser.add_argument("--continue-on-empty-rounds", default="yes", help="Do not stop immediately when a round inserts nothing; widen search and retry")
    parser.add_argument("--max-consecutive-empty-rounds", type=int, default=6, help="Hard stop after this many consecutive empty rounds")
    parser.add_argument("--candidate-limit-step", type=int, default=6, help="How much to widen candidate_limit after an empty round")
    parser.add_argument("--candidate-limit-max", type=int, default=60, help="Upper bound for adaptive candidate_limit widening")
    parser.add_argument("--quarantine-failures", default="yes", help="Exclude failed/rolled-back families from later rounds in the same run")
    parser.add_argument("--candidate-request-retries", type=int, default=2, help="Extra outer retries per candidate around Gemini generation")
    parser.add_argument("--candidate-retry-sleep", type=float, default=2.0, help="Sleep seconds between outer candidate retries")
    parser.add_argument("--run-through", default="no", help="Convenience mode: keep going, quarantine failures, retry empty rounds, and finish the run")
    parser.add_argument(
        "--model-candidates",
        default=os.environ.get("GEMINI_MODEL_CANDIDATES", os.environ.get("GEMINI_MODEL", "gemini-2.5-pro,gemini-2.5-flash,gemini-2.5-flash-lite")),
        help="Comma-separated Gemini model candidates",
    )
    args = parser.parse_args()

    canonical_db = Path(args.db).resolve()
    stage_db = Path(args.stage_db).resolve()
    report_root = Path(args.report_dir).resolve()
    run_id = now_ts()
    report_dir = report_root / run_id
    report_dir.mkdir(parents=True, exist_ok=True)

    load_env(ROOT)
    if yesno(args.chaos_mode):
        args.allow_generic = "yes"
        args.min_confidence = min(args.min_confidence, 0.58)
        if args.benchmark_every < 5:
            args.benchmark_every = 5

    if yesno(args.run_through):
        args.rollback_on_regression = "yes"
        args.stop_on_regression = "yes"
        args.continue_on_empty_rounds = "yes"
        args.quarantine_failures = "yes"
        if int(args.max_consecutive_empty_rounds) < 8:
            args.max_consecutive_empty_rounds = 8

    ensure_stage_db(canonical_db, stage_db, reset=args.reset_stage)
    shutil.copy2(stage_db, report_dir / stage_db.name)

    api_key = (os.environ.get("GEMINI_API_KEY") or "").strip()
    model_candidates = [m.strip() for m in str(args.model_candidates).split(",") if m.strip()]
    gemini = GeminiClient(api_key=api_key, preferred_models=model_candidates)
    include_families = parse_family_csv(args.include_families)
    exclude_families = parse_family_csv(args.exclude_families)
    runtime_exclude_families: set[str] = set(exclude_families)
    quarantined_records: list[dict[str, Any]] = []

    conn = sqlite3.connect(str(stage_db))
    conn.row_factory = sqlite3.Row
    items: list[dict[str, Any]] = []
    baseline_benchmark: dict[str, Any] | None = None
    baseline_verify: dict[str, Any] | None = None
    inserted_count = 0
    created_pattern_count = 0
    stopped_reason = "completed"
    try:
        start_state = current_state(conn)
        if yesno(args.benchmark):
            baseline_benchmark = run_benchmark(stage_db, report_dir, "baseline")
            baseline_verify = run_verify(stage_db, report_dir, "baseline")
        rounds_run = 0
        consecutive_empty_rounds = 0
        adaptive_candidate_limit = int(args.candidate_limit)
        while True:
            rounds_run += 1
            state_before_round = current_state(conn)
            if state_before_round["family_coverage"] >= int(args.family_target):
                stopped_reason = "family_target_reached_before_round"
                break
            if rounds_run > int(args.max_rounds):
                stopped_reason = "max_rounds_reached"
                break

            candidates = select_candidates(
                conn,
                candidate_limit=adaptive_candidate_limit,
                allow_generic=yesno(args.allow_generic),
                include_families=include_families or None,
                exclude_families=exclude_families or None,
                runtime_exclude_families=runtime_exclude_families or None,
            )
            plan_path = report_dir / f"candidate_plan_round_{rounds_run:02d}.json"
            plan_path.write_text(json.dumps([p.__dict__ for p in candidates], ensure_ascii=False, indent=2), encoding="utf-8")
            if not candidates:
                stopped_reason = "no_candidates"
                break
            if args.plan_only:
                stopped_reason = "plan_only"
                break

            inserted_this_round = 0
            for packet in candidates:
                if current_state(conn)["family_coverage"] >= int(args.family_target):
                    stopped_reason = "family_target_reached"
                    break
                system, prompt = make_prompt(packet, allow_create_new=yesno(args.allow_create_new), allow_generic=yesno(args.allow_generic))
                image_path = resolve_image_path(ROOT, packet.image_filename)
                item: dict[str, Any] = {
                    "round": rounds_run,
                    "family_name": packet.family_name,
                    "page_no": packet.page_no,
                    "priority_score": packet.priority_score,
                    "image_filename": packet.image_filename,
                }
                try:
                    payload = None
                    model_name = ""
                    outer_errors: list[str] = []
                    for outer_try in range(1, int(args.candidate_request_retries) + 2):
                        try:
                            payload, model_name = gemini.generate_json(prompt, system_instruction=system, image_path=image_path)
                            break
                        except Exception as e:
                            outer_errors.append(f"try {outer_try}: {e}")
                            if outer_try <= int(args.candidate_request_retries):
                                time.sleep(float(args.candidate_retry_sleep) * outer_try)
                                continue
                            raise RuntimeError(" ; ".join(outer_errors[-3:]))
                    item["model_name"] = model_name
                    item["decision"] = payload.get("decision")
                    item["confidence"] = payload.get("confidence")
                    ok, errors = validate_gemini_payload(payload, packet, allow_generic=yesno(args.allow_generic), min_confidence=float(args.min_confidence))
                    if not ok:
                        item["status"] = "validation_failed"
                        item["note"] = " | ".join(errors)
                        if yesno(args.quarantine_failures):
                            runtime_exclude_families.add(packet.family_name_norm)
                            item["quarantined"] = True
                            quarantined_records.append({"family_name": packet.family_name, "reason": item["status"], "note": item["note"]})
                        items.append(item)
                        continue
                    if str(payload.get("decision") or "").strip().lower() in {"hold", "reject"}:
                        item["status"] = str(payload.get("decision") or "").strip().lower()
                        item["note"] = str(payload.get("notes_text") or "")[:300]
                        if yesno(args.quarantine_failures):
                            runtime_exclude_families.add(packet.family_name_norm)
                            item["quarantined"] = True
                            quarantined_records.append({"family_name": packet.family_name, "reason": item["status"], "note": item["note"]})
                        items.append(item)
                        continue
                    candidate_backup = report_dir / f"candidate_backup_r{rounds_run:02d}_{safe_name(packet.family_name)}.db"
                    shutil.copy2(stage_db, candidate_backup)
                    conn.execute("BEGIN")
                    ins = insert_seed(conn, packet, payload, model_name=model_name)
                    conn.commit()
                    inserted_flag = 1 if ins.get("status") == "inserted" else 0
                    created_flag = 1 if ins.get("created_pattern") else 0
                    inserted_count += inserted_flag
                    created_pattern_count += created_flag
                    inserted_this_round += inserted_flag
                    item.update(ins)
                    item["note"] = str(payload.get("notes_text") or "")[:300]

                    if int(args.snapshot_every) > 0 and inserted_count > 0 and inserted_count % int(args.snapshot_every) == 0:
                        shutil.copy2(stage_db, report_dir / f"snapshot_after_{inserted_count:03d}.db")

                    if yesno(args.benchmark) and int(args.benchmark_every) > 0 and inserted_count > 0 and inserted_count % int(args.benchmark_every) == 0:
                        suffix = f"round{rounds_run:02d}_after_{inserted_count:03d}"
                        cur_bench = run_benchmark(stage_db, report_dir, suffix)
                        run_verify(stage_db, report_dir, suffix)
                        if baseline_benchmark and yesno(args.stop_on_regression):
                            should_stop, why = should_stop_for_regression(
                                baseline_benchmark,
                                cur_bench,
                                allowed_top1_drop=float(args.allowed_top1_drop),
                                allowed_violation_increase=int(args.allowed_violation_increase),
                            )
                            if should_stop and yesno(args.rollback_on_regression):
                                try:
                                    conn.close()
                                except Exception:
                                    pass
                                shutil.copy2(candidate_backup, stage_db)
                                conn = sqlite3.connect(str(stage_db))
                                conn.row_factory = sqlite3.Row
                                inserted_count -= inserted_flag
                                created_pattern_count -= created_flag
                                inserted_this_round -= inserted_flag
                                item["status"] = "rolled_back_regression"
                                item["note"] = why
                                item["rolled_back"] = True
                                if yesno(args.quarantine_failures):
                                    runtime_exclude_families.add(packet.family_name_norm)
                                    item["quarantined"] = True
                                    quarantined_records.append({"family_name": packet.family_name, "reason": item["status"], "note": item["note"]})
                                items.append(item)
                                continue
                            if should_stop:
                                item["status"] = "regression_stop"
                                item["note"] = why
                                items.append(item)
                                stopped_reason = f"regression_stop: {why}"
                                break
                    items.append(item)
                except Exception as e:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    item["status"] = "error"
                    item["note"] = str(e)[:500]
                    if yesno(args.quarantine_failures):
                        runtime_exclude_families.add(packet.family_name_norm)
                        item["quarantined"] = True
                        quarantined_records.append({"family_name": packet.family_name, "reason": item["status"], "note": item["note"]})
                    items.append(item)
                    continue
            if stopped_reason.startswith("regression_stop") or stopped_reason == "family_target_reached":
                break
            if inserted_this_round == 0:
                consecutive_empty_rounds += 1
                if yesno(args.continue_on_empty_rounds) and consecutive_empty_rounds <= int(args.max_consecutive_empty_rounds):
                    adaptive_candidate_limit = min(int(args.candidate_limit_max), adaptive_candidate_limit + int(args.candidate_limit_step))
                    items.append({
                        "round": rounds_run,
                        "family_name": "",
                        "status": "empty_round_retry",
                        "decision": "",
                        "target_family_name": "",
                        "model_name": "",
                        "created_pattern": False,
                        "note": f"empty round; widening candidate_limit to {adaptive_candidate_limit}; consecutive_empty_rounds={consecutive_empty_rounds}",
                    })
                    continue
                stopped_reason = "max_consecutive_empty_rounds_reached" if yesno(args.continue_on_empty_rounds) else "no_insertions_this_round"
                break
            consecutive_empty_rounds = 0

        final_state = current_state(conn)
    finally:
        conn.close()

    summary = {
        "run_id": run_id,
        "canonical_db": str(canonical_db),
        "stage_db": str(stage_db),
        "family_target": int(args.family_target),
        "candidate_limit": int(args.candidate_limit),
        "max_rounds": int(args.max_rounds),
        "allow_generic": yesno(args.allow_generic),
        "allow_create_new": yesno(args.allow_create_new),
        "min_confidence": float(args.min_confidence),
        "benchmark_enabled": yesno(args.benchmark),
        "start_state": start_state,
        "baseline_benchmark_summary": (baseline_benchmark or {}).get("summary") if baseline_benchmark else None,
        "baseline_verify": baseline_verify,
        "inserted_count": inserted_count,
        "created_pattern_count": created_pattern_count,
        "final_state": final_state,
        "stopped_reason": stopped_reason,
        "report_dir": str(report_dir),
        "models_requested": model_candidates,
        "models_resolved": gemini.resolve_models() if api_key else [],
        "include_families": sorted(include_families),
        "exclude_families": sorted(exclude_families),
        "rollback_on_regression": yesno(args.rollback_on_regression),
        "continue_on_empty_rounds": yesno(args.continue_on_empty_rounds),
        "max_consecutive_empty_rounds": int(args.max_consecutive_empty_rounds),
        "adaptive_candidate_limit_final": adaptive_candidate_limit,
        "quarantine_failures": yesno(args.quarantine_failures),
        "quarantined_family_count": len({q['family_name'] for q in quarantined_records}),
        "quarantined_families": sorted({q['family_name'] for q in quarantined_records}),
        "run_through": yesno(args.run_through),
    }
    (report_dir / 'quarantined_families.json').write_text(json.dumps(quarantined_records, ensure_ascii=False, indent=2), encoding='utf-8')
    write_run_summary(report_dir, summary, items)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"RUN_ITEMS_CSV={report_dir / 'run_items.csv'}")
    print(f"RUN_SUMMARY_JSON={report_dir / 'run_summary.json'}")
    print(f"RUN_SUMMARY_MD={report_dir / 'run_summary.md'}")


if __name__ == "__main__":
    main()
