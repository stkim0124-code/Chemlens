from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_MODEL = "gemini-2.5-pro"


# ------------------------
# generic helpers
# ------------------------

def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    return conn


def _load_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    env_text = None
    for enc in ("utf-8", "utf-8-sig", "cp949", "latin-1"):
        try:
            env_text = path.read_text(encoding=enc)
            break
        except Exception:
            continue
    if env_text is None:
        env_text = path.read_bytes().decode("ascii", errors="ignore")
    for line in env_text.splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_") or "item"


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def _strip_fences(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?", "", t).strip()
        if t.endswith("```"):
            t = t[:-3].strip()
    return t


def _extract_json_object(text: str) -> dict[str, Any]:
    t = _strip_fences(text)
    try:
        payload = json.loads(t)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    m = re.search(r"\{.*\}", t, flags=re.S)
    if not m:
        raise ValueError("No JSON object found in Gemini response")
    payload = json.loads(m.group(0))
    if not isinstance(payload, dict):
        raise ValueError("Gemini response JSON root is not an object")
    return payload


def _extract_candidate_text(data: dict[str, Any]) -> str:
    candidates = data.get("candidates") or []
    cand0 = candidates[0] if candidates else {}
    content = cand0.get("content") or {}
    parts = content.get("parts") or []
    texts: list[str] = []
    for p in parts:
        if isinstance(p, dict):
            txt = p.get("text")
            if isinstance(txt, str) and txt.strip():
                texts.append(txt.strip())
    if texts:
        return "\n".join(texts)
    finish_reason = cand0.get("finishReason") or data.get("promptFeedback", {}).get("blockReason") or "unknown"
    raise RuntimeError(f"Gemini response did not contain text parts (finish_reason={finish_reason})")


def _payloads_for(model: str, prompt: str, temperature: float, max_tokens: int) -> list[dict[str, Any]]:
    capped_tokens = max(256, min(max_tokens, 768))
    payloads: list[dict[str, Any]] = []
    budgets = [128, 256] if model.startswith("gemini-2.5-pro") else [None]
    for budget in budgets:
        gen_cfg: dict[str, Any] = {
            "responseMimeType": "application/json",
            "temperature": temperature,
            "maxOutputTokens": capped_tokens,
        }
        if budget is not None:
            gen_cfg["thinkingConfig"] = {"thinkingBudget": budget}
        payloads.append({
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": gen_cfg,
        })
        gen_cfg2 = dict(gen_cfg)
        gen_cfg2.pop("responseMimeType", None)
        payloads.append({
            "contents": [{"parts": [{"text": prompt + " Return plain JSON only."}]}],
            "generationConfig": gen_cfg2,
        })
    return payloads


def _request_gemini(api_key: str, model: str, prompt: str, max_tokens: int = 640, temperature: float = 0.2) -> tuple[str, dict[str, Any]]:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    last_error: Exception | None = None
    for payload in _payloads_for(model, prompt, temperature, max_tokens):
        try:
            resp = requests.post(url, json=payload, timeout=240)
            body = resp.text[:6000]
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}: {body}")
            data = resp.json()
            text = _extract_candidate_text(data)
            return text, data
        except Exception as exc:
            last_error = exc
            continue
    raise RuntimeError(f"Gemini request failed after retries: {last_error}")


def _unwrap_bench_payload(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict) and "summary" in payload and isinstance(payload.get("summary"), dict):
        data = dict(payload["summary"])
        data["rows"] = payload.get("rows", [])
        return data
    return payload if isinstance(payload, dict) else {"raw": payload}


def _run_benchmark(backend_root: Path, db_path: Path, benchmark_path: Path, report_dir: Path, prefix: str) -> dict[str, Any]:
    json_out = report_dir / f"{prefix}.json"
    csv_out = report_dir / f"{prefix}.csv"
    md_out = report_dir / f"{prefix}.md"
    cmd = [
        sys.executable,
        str(backend_root / "run_named_reaction_benchmark_small.py"),
        "--db", str(db_path),
        "--benchmark", str(benchmark_path),
        "--json-out", str(json_out),
        "--csv-out", str(csv_out),
        "--report-md", str(md_out),
    ]
    cp = subprocess.run(cmd, cwd=str(backend_root), capture_output=True, text=True, encoding="utf-8", errors="replace")
    if cp.returncode != 0:
        raise RuntimeError(f"benchmark failed\nSTDOUT:\n{cp.stdout}\nSTDERR:\n{cp.stderr}")
    payload = json.loads(json_out.read_text(encoding="utf-8"))
    data = _unwrap_bench_payload(payload)
    data["db_used"] = str(db_path)
    return data


def _table_cols(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _common_non_id_cols(src_cols: Iterable[str], dst_cols: Iterable[str]) -> list[str]:
    dst = set(dst_cols)
    return [c for c in src_cols if c in dst and c != "id"]


def _next_id(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM {table}").fetchone()[0])


def _get_scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _state(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        "queryable": _get_scalar(conn, "SELECT COUNT(*) FROM extract_molecules WHERE queryable=1"),
        "family_coverage": _get_scalar(conn, "SELECT COUNT(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1 AND COALESCE(reaction_family_name,'')<>''"),
        "reaction_extracts": _get_scalar(conn, "SELECT COUNT(*) FROM reaction_extracts"),
        "extract_molecules_total": _get_scalar(conn, "SELECT COUNT(*) FROM extract_molecules"),
    }


# ------------------------
# family registry / prompt
# ------------------------

@dataclass
class FamilyInfo:
    family_name: str
    family_name_norm: str
    family_class: str
    transformation_type: str
    mechanism_type: str
    key_reagents_clue: str
    description_short: str
    synonym_names: str
    evidence_extract_count: int
    overview_count: int
    application_count: int
    mechanism_count: int
    priority_score: float


def _resolved_families(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT DISTINCT reaction_family_name FROM extract_molecules WHERE queryable=1 AND COALESCE(reaction_family_name,'')<>''"
    ).fetchall()
    return {r[0] for r in rows if r[0]}


def _load_registry(conn: sqlite3.Connection) -> list[FamilyInfo]:
    cols = set(_table_cols(conn, "reaction_family_patterns"))
    # read only known columns that may exist
    select_cols = [
        "family_name", "family_name_norm", "family_class", "transformation_type", "mechanism_type",
        "key_reagents_clue", "description_short", "synonym_names", "evidence_extract_count",
        "overview_count", "application_count", "mechanism_count",
    ]
    use = [c for c in select_cols if c in cols]
    rows = conn.execute(f"SELECT {', '.join(use)} FROM reaction_family_patterns ORDER BY family_name").fetchall()
    dedup: dict[str, FamilyInfo] = {}
    for row in rows:
        name = row[0]
        if not name:
            continue
        fam_norm = row[1] if len(row) > 1 and row[1] else _norm(name)
        idx = {c: row[i] for i, c in enumerate(use)}
        score = float(idx.get("overview_count") or 0) * 3 + float(idx.get("mechanism_count") or 0) * 2 + float(idx.get("application_count") or 0) * 1.5 + float(idx.get("evidence_extract_count") or 0)
        score += 5 if idx.get("description_short") else 0
        score += 3 if idx.get("key_reagents_clue") else 0
        fi = FamilyInfo(
            family_name=name,
            family_name_norm=fam_norm,
            family_class=idx.get("family_class") or "",
            transformation_type=idx.get("transformation_type") or "",
            mechanism_type=idx.get("mechanism_type") or "",
            key_reagents_clue=idx.get("key_reagents_clue") or "",
            description_short=idx.get("description_short") or "",
            synonym_names=idx.get("synonym_names") or "",
            evidence_extract_count=int(idx.get("evidence_extract_count") or 0),
            overview_count=int(idx.get("overview_count") or 0),
            application_count=int(idx.get("application_count") or 0),
            mechanism_count=int(idx.get("mechanism_count") or 0),
            priority_score=score,
        )
        key = _norm(name)
        prev = dedup.get(key)
        if prev is None or fi.priority_score > prev.priority_score:
            dedup[key] = fi
    return list(dedup.values())


def _stage_hint(stage_conn: sqlite3.Connection, family_name: str) -> str:
    row = stage_conn.execute(
        "SELECT reactants_text, products_text, reagents_text, notes_text, reactant_smiles, product_smiles FROM reaction_extracts WHERE reaction_family_name=? ORDER BY id DESC LIMIT 1",
        (family_name,),
    ).fetchone()
    if row is None:
        return ""
    parts = []
    if row[0]: parts.append(f"stage_reactants={row[0]}")
    if row[1]: parts.append(f"stage_products={row[1]}")
    if row[2]: parts.append(f"stage_reagents={row[2]}")
    if row[4] or row[5]: parts.append(f"avoid copying existing stage topology; current_stage_smiles={row[4] or ''}>>{row[5] or ''}")
    return " | ".join(parts)


def _family_specific_guidance(family_name: str) -> str:
    key = _norm(family_name)
    mapping = {
        _norm("Claisen Condensation / Claisen Reaction"): "Use an intermolecular Claisen condensation that forms a beta-keto ester. Avoid Claisen rearrangement entirely.",
        _norm("Krapcho Dealkoxycarbonylation"): "Use a textbook Krapcho dealkoxycarbonylation on a simple beta-keto ester or malonate derivative with loss of one alkoxycarbonyl group.",
        _norm("Horner-Wadsworth-Emmons Olefination"): "Use a simple phosphonate-stabilized HWE olefination making an E-alkene. Keep the substrate acyclic and textbook-simple.",
        _norm("Regitz Diazo Transfer"): "Use a simple active methylene substrate undergoing diazo transfer. Keep arene-free and avoid Buchner-like diazo/arene combinations.",
        _norm("Enyne Metathesis"): "Use a simple ring-closing enyne metathesis example. Avoid decarboxylation, deoxygenation, Barton-type radical motifs, or strange organometallic fragments in SMILES.",
        _norm("Hofmann-Loffler-Freytag Reaction"): "Use a simple N-haloamine to cyclic amine example. Avoid Barton decarboxylation/deoxygenation motifs.",
        _norm("Mitsunobu Reaction"): "Use a simple inversion/esterification Mitsunobu example without Barton-like deoxygenation context.",
    }
    return mapping.get(key, "Use the simplest textbook, RDKit-parseable canonical example. Avoid generic/arene-heavy/reactive motifs that could collide with unrelated named reactions.")


def _build_prompt(fam: FamilyInfo, stage_hint: str) -> str:
    meta = []
    if fam.family_class:
        meta.append(f"family_class={fam.family_class}")
    if fam.transformation_type:
        meta.append(f"transformation_type={fam.transformation_type}")
    if fam.mechanism_type:
        meta.append(f"mechanism_type={fam.mechanism_type}")
    if fam.key_reagents_clue:
        meta.append(f"key_reagents={fam.key_reagents_clue}")
    if fam.description_short:
        meta.append(f"description={fam.description_short}")
    if fam.synonym_names:
        meta.append(f"synonyms={fam.synonym_names}")
    meta_txt = " | ".join(meta)

    return f"""
You are generating ONE benchmark-safe canonical seed for the named reaction family \"{fam.family_name}\".

Goal:
Produce a single clear textbook example that helps a chemistry search engine recognize this family without hijacking unrelated named-reaction benchmark queries.

Rules:
- Must be a canonical textbook example of this named reaction family.
- Keep the reaction as simple, acyclic, and RDKit-parseable as possible unless a ring is essential.
- Avoid generic arene-rich, diazo-arene, ring-expansion, decarboxylation, deoxygenation, or radical motifs unless they are absolutely essential for this exact family.
- Avoid copying the current stage seed topology.
- Provide only one candidate in JSON.
- Reactants/products must be chemically reasonable and parse with RDKit.
- Prefer 1-2 reactants and 1 product. Optional reagent_smiles may be empty or omitted.

Family guidance:
{_family_specific_guidance(fam.family_name)}

Metadata:
{meta_txt or '(none)'}
{stage_hint or ''}

Return JSON only in exactly this schema:
{{
  "family": "{fam.family_name}",
  "candidate": {{
    "substrate_smiles": ["..."],
    "product_smiles": ["..."],
    "optional_reagent_smiles": ["..."],
    "rationale": "...",
    "collision_avoidance_note": "..."
  }}
}}
""".strip()


# ------------------------
# candidate validation / insert
# ------------------------

def _validate_smiles_list(smiles_list: Iterable[str]) -> list[str]:
    from rdkit import Chem
    cleaned: list[str] = []
    for s in smiles_list:
        if not s or not str(s).strip():
            continue
        raw = str(s).strip()
        mol = Chem.MolFromSmiles(raw)
        if mol is None:
            raise ValueError(f"RDKit parse failed: {raw}")
        cleaned.append(Chem.MolToSmiles(mol))
    return cleaned


def _parse_candidate(raw_text: str, expected_family: str) -> dict[str, Any]:
    obj = _extract_json_object(raw_text)
    fam = obj.get("family") or expected_family
    cand = obj.get("candidate") or obj.get("candidates")
    if isinstance(cand, list):
        cand = cand[0] if cand else {}
    if not isinstance(cand, dict):
        raise ValueError("candidate object missing in Gemini response")
    reactants = _validate_smiles_list(cand.get("substrate_smiles") or [])
    products = _validate_smiles_list(cand.get("product_smiles") or [])
    reagents = _validate_smiles_list(cand.get("optional_reagent_smiles") or [])
    if not reactants or not products:
        raise ValueError("candidate missing reactant/product smiles")
    return {
        "family": fam,
        "substrates": reactants,
        "products": products,
        "reagents": reagents,
        "rationale": str(cand.get("rationale") or "").strip(),
        "collision_avoidance_note": str(cand.get("collision_avoidance_note") or "").strip(),
        "raw_text": raw_text,
    }


def _choose_scheme_candidate_id(conn: sqlite3.Connection) -> int:
    try:
        row = conn.execute("SELECT MIN(id) FROM scheme_candidates").fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except sqlite3.Error:
        pass
    return 1


def _insert_candidate(conn: sqlite3.Connection, family: str, candidate: dict[str, Any], model: str, structure_source: str) -> tuple[int, list[int]]:
    re_cols = _table_cols(conn, "reaction_extracts")
    em_cols = _table_cols(conn, "extract_molecules")

    re_id = _next_id(conn, "reaction_extracts")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    reaction_insert: dict[str, Any] = {
        "id": re_id,
        "scheme_candidate_id": _choose_scheme_candidate_id(conn),
        "reaction_family_name": family,
        "reaction_family_name_norm": _norm(family),
        "extract_kind": "canonical_overview",
        "transformation_text": f"{', '.join(candidate['substrates'])} -> {', '.join(candidate['products'])}",
        "reactants_text": ", ".join(candidate["substrates"]),
        "products_text": ", ".join(candidate["products"]),
        "reagents_text": ", ".join(candidate["reagents"]),
        "conditions_text": candidate.get("collision_avoidance_note", ""),
        "notes_text": candidate.get("rationale", ""),
        "reactant_smiles": ".".join(candidate["substrates"]),
        "product_smiles": ".".join(candidate["products"]),
        "smiles_confidence": 0.95,
        "extraction_confidence": 0.90,
        "parse_status": "parsed",
        "promote_decision": "promote",
        "extractor_model": model,
        "extractor_prompt_version": "gemini_expand_v5",
        "extraction_raw_json": json.dumps(candidate, ensure_ascii=False),
        "created_at": now,
        "updated_at": now,
    }
    re_use_cols = [c for c in reaction_insert if c in re_cols]
    conn.execute(
        f"INSERT INTO reaction_extracts ({', '.join(re_use_cols)}) VALUES ({', '.join('?' for _ in re_use_cols)})",
        tuple(reaction_insert[c] for c in re_use_cols),
    )

    inserted_em: list[int] = []

    def add_em(smiles: str, role: str, queryable: int) -> None:
        em_id = _next_id(conn, "extract_molecules")
        em_insert: dict[str, Any] = {
            "id": em_id,
            "extract_id": re_id,
            "role": role,
            "smiles": smiles,
            "smiles_kind": "canonicalized",
            "quality_tier": 1,
            "reaction_family_name": family,
            "source_zip": "gemini_expand_v5",
            "page_no": 0,
            "queryable": queryable,
            "note_text": candidate.get("rationale", ""),
            "morgan_fp": None,
            "normalized_text": smiles,
            "source_field": role,
            "structure_source": structure_source,
            "alias_id": None,
            "fg_tags": None,
            "role_confidence": 0.95,
            "created_at": now,
        }
        em_use_cols = [c for c in em_insert if c in em_cols]
        conn.execute(
            f"INSERT INTO extract_molecules ({', '.join(em_use_cols)}) VALUES ({', '.join('?' for _ in em_use_cols)})",
            tuple(em_insert[c] for c in em_use_cols),
        )
        inserted_em.append(em_id)

    for s in candidate["substrates"]:
        add_em(s, "reactant", 1)
    for s in candidate["products"]:
        add_em(s, "product", 1)
    for s in candidate["reagents"]:
        add_em(s, "reagent", 0)

    return re_id, inserted_em


# ------------------------
# loop orchestration
# ------------------------

def _pick_round_families(unresolved: list[FamilyInfo], per_round: int, round_no: int) -> list[FamilyInfo]:
    if len(unresolved) <= per_round:
        return unresolved
    ordered = sorted(unresolved, key=lambda x: (-x.priority_score, x.family_name))
    start = ((round_no - 1) * per_round) % len(ordered)
    # wraparound chunk
    out: list[FamilyInfo] = []
    idx = start
    while len(out) < per_round and len(out) < len(ordered):
        out.append(ordered[idx])
        idx = (idx + 1) % len(ordered)
    return out


def _write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Continuous v5 expansion loop using Gemini + benchmark screening + frozen apply reuse")
    ap.add_argument("--db", default=str(SCRIPT_DIR / "app" / "labint.db"))
    ap.add_argument("--stage-db", default=str(SCRIPT_DIR / "app" / "labint_v5_stage.db"))
    ap.add_argument("--benchmark-file", default=str(SCRIPT_DIR / "benchmark" / ("named_reaction_benchmark_v4.json" if (SCRIPT_DIR / "benchmark" / "named_reaction_benchmark_v4.json").exists() else "named_reaction_benchmark_small.json")))
    ap.add_argument("--report-dir", default=str(SCRIPT_DIR / "reports" / "gemini_expand_v5"))
    ap.add_argument("--family-target", type=int, default=305)
    ap.add_argument("--families-per-round", type=int, default=12)
    ap.add_argument("--max-attempts", type=int, default=3)
    ap.add_argument("--max-rounds", type=int, default=999999)
    ap.add_argument("--max-empty-rounds", type=int, default=999999)
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--temperature", type=float, default=0.2)
    ap.add_argument("--max-tokens", type=int, default=640)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    random.seed(args.seed)
    backend_root = SCRIPT_DIR
    canonical_db = Path(args.db).resolve()
    stage_db = Path(args.stage_db).resolve()
    benchmark_path = Path(args.benchmark_file).resolve()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = Path(args.report_dir).resolve() / run_id
    frozen_dir = report_dir / "frozen_candidates"
    report_dir.mkdir(parents=True, exist_ok=True)
    frozen_dir.mkdir(parents=True, exist_ok=True)

    env = _load_env(backend_root / ".env")
    api_key = (env.get("GEMINI_API_KEY") or os.environ.get("GEMINI_API_KEY") or "").strip()
    model = (env.get("GEMINI_MODEL") or args.model or DEFAULT_MODEL).strip()
    if not model or model.startswith("gemini-1.5"):
        model = DEFAULT_MODEL
    if not api_key:
        print("[ERROR] GEMINI_API_KEY not found in backend/.env or environment")
        return 2

    print("=" * 72)
    print("CONTINUOUS GEMINI EXPAND V5")
    print("=" * 72)
    print(f"canonical:         {canonical_db}")
    print(f"stage_db:          {stage_db}")
    print(f"benchmark:         {benchmark_path}")
    print(f"model:             {model}")
    print(f"family_target:     {args.family_target}")
    print(f"families_per_round:{args.families_per_round}")
    print(f"max_attempts:      {args.max_attempts}")
    print(f"max_rounds:        {args.max_rounds}")
    print(f"max_empty_rounds:  {args.max_empty_rounds}")
    print(f"report_dir:        {report_dir}")

    base_conn = _connect(canonical_db)
    baseline_bench = _run_benchmark(backend_root, canonical_db, benchmark_path, report_dir, "baseline")
    baseline_state = _state(base_conn)
    print(f"[baseline] top1={baseline_bench['top1_accuracy']:.4f} top3={baseline_bench['top3_accuracy']:.4f} violations={baseline_bench['disallow_top3_violations']}")
    print(f"[baseline_state] queryable={baseline_state['queryable']} family_coverage={baseline_state['family_coverage']} reaction_extracts={baseline_state['reaction_extracts']}")

    stage_conn = _connect(stage_db) if stage_db.exists() else None

    summary: dict[str, Any] = {
        "run_id": run_id,
        "canonical": str(canonical_db),
        "stage_db": str(stage_db),
        "benchmark": str(benchmark_path),
        "model": model,
        "baseline_benchmark": baseline_bench,
        "baseline_state": baseline_state,
        "rounds": [],
        "applied_families": [],
    }

    empty_rounds = 0
    for round_no in range(1, args.max_rounds + 1):
        # recompute from current canonical each round so reruns naturally resume
        curr_conn = _connect(canonical_db)
        registry = _load_registry(curr_conn)
        resolved = _resolved_families(curr_conn)
        unresolved = [f for f in registry if f.family_name not in resolved]
        curr_state = _state(curr_conn)
        curr_conn.close()

        if curr_state["family_coverage"] >= args.family_target or not unresolved:
            print("[STOP] target reached or no unresolved families remain")
            break

        round_fams = _pick_round_families(unresolved, args.families_per_round, round_no)
        print("\n" + "-" * 72)
        print(f"[ROUND {round_no}] unresolved_total={len(unresolved)} current_coverage={curr_state['family_coverage']} :: {', '.join(f.family_name for f in round_fams)}")
        print("-" * 72)
        round_entry: dict[str, Any] = {"round": round_no, "coverage_before": curr_state["family_coverage"], "families": []}
        round_applied = 0

        for fam in round_fams:
            stage_hint = _stage_hint(stage_conn, fam.family_name) if stage_conn is not None else ""
            prompt = _build_prompt(fam, stage_hint)
            fam_entry: dict[str, Any] = {"family": fam.family_name, "attempts": [], "applied": False}
            print(f"\n[{fam.family_name}]")
            chosen_candidate: dict[str, Any] | None = None
            chosen_attempt = None
            for attempt in range(1, args.max_attempts + 1):
                attempt_entry: dict[str, Any] = {"attempt": attempt}
                try:
                    raw_text, api_response = _request_gemini(api_key, model, prompt, max_tokens=args.max_tokens, temperature=args.temperature)
                    candidate = _parse_candidate(raw_text, fam.family_name)
                    attempt_dir = report_dir / f"round{round_no:03d}_{_safe_name(fam.family_name)}_attempt{attempt:02d}"
                    attempt_dir.mkdir(parents=True, exist_ok=True)
                    _write_json(attempt_dir / "candidate.json", candidate)
                    _write_json(attempt_dir / "api_response.json", api_response)
                    # screen in temp copy without mutating canonical
                    tmp_fd, tmp_name = tempfile.mkstemp(prefix="chemlens_expand_v5_", suffix=".db")
                    os.close(tmp_fd)
                    tmp_db = Path(tmp_name)
                    shutil.copy2(canonical_db, tmp_db)
                    tmp_conn = _connect(tmp_db)
                    _insert_candidate(tmp_conn, fam.family_name, candidate, model, "gemini_expand_v5_seed")
                    tmp_conn.commit()
                    tmp_conn.close()
                    bench = _run_benchmark(backend_root, tmp_db, benchmark_path, attempt_dir, f"bench_round{round_no:03d}_attempt{attempt:02d}")
                    tmp_db.unlink(missing_ok=True)
                    attempt_entry["candidate"] = candidate
                    attempt_entry["bench"] = {k: bench.get(k) for k in ("top1_accuracy", "top3_accuracy", "disallow_top3_violations")}
                    pass_ok = (float(bench.get("top1_accuracy", 0.0)) == 1.0 and float(bench.get("top3_accuracy", 0.0)) == 1.0 and int(bench.get("disallow_top3_violations", 999)) == 0)
                    if pass_ok:
                        frozen_path = frozen_dir / f"{_safe_name(fam.family_name)}.json"
                        _write_json(frozen_path, {"family": fam.family_name, "attempt": attempt, "candidate": candidate, "screen_bench": bench})
                        chosen_candidate = candidate
                        chosen_attempt = attempt
                        print(f"  attempt {attempt}: top1={bench['top1_accuracy']:.4f} top3={bench['top3_accuracy']:.4f} violations={bench['disallow_top3_violations']} PASS")
                        break
                    print(f"  attempt {attempt}: top1={bench['top1_accuracy']:.4f} top3={bench['top3_accuracy']:.4f} violations={bench['disallow_top3_violations']} FAIL")
                except Exception as exc:
                    attempt_entry["error"] = str(exc)
                    print(f"  attempt {attempt}: ERROR {exc}")
                fam_entry["attempts"].append(attempt_entry)

            if chosen_candidate is None:
                round_entry["families"].append(fam_entry)
                continue

            # apply exact frozen candidate, not regenerate
            backup_path = canonical_db.with_name(f"labint.backup_before_expand_v5_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_safe_name(fam.family_name)[:30]}.db")
            shutil.copy2(canonical_db, backup_path)
            try:
                can_conn = _connect(canonical_db)
                _insert_candidate(can_conn, fam.family_name, chosen_candidate, model, "gemini_expand_v5_seed")
                can_conn.commit()
                can_conn.close()
                final_bench = _run_benchmark(backend_root, canonical_db, benchmark_path, report_dir, f"after_apply_round{round_no:03d}_{_safe_name(fam.family_name)}")
                if not (float(final_bench.get("top1_accuracy", 0.0)) == 1.0 and float(final_bench.get("top3_accuracy", 0.0)) == 1.0 and int(final_bench.get("disallow_top3_violations", 999)) == 0):
                    shutil.copy2(backup_path, canonical_db)
                    print(f"  apply rollback: post-apply benchmark failed for {fam.family_name}")
                else:
                    fam_entry["applied"] = True
                    fam_entry["applied_attempt"] = chosen_attempt
                    fam_entry["backup"] = str(backup_path)
                    fam_entry["final_bench"] = {k: final_bench.get(k) for k in ("top1_accuracy", "top3_accuracy", "disallow_top3_violations")}
                    summary["applied_families"].append(fam.family_name)
                    round_applied += 1
                    print(f"[{fam.family_name}] APPLIED")
            except Exception as exc:
                shutil.copy2(backup_path, canonical_db)
                fam_entry["apply_error"] = str(exc)
                print(f"  apply error: {exc} -> restored backup")
            round_entry["families"].append(fam_entry)

        round_entry["applied_count"] = round_applied
        summary["rounds"].append(round_entry)
        _write_json(report_dir / "campaign_state.json", summary)
        if round_applied == 0:
            empty_rounds += 1
        else:
            empty_rounds = 0
        print(f"\n[ROUND {round_no} RESULT] applied={round_applied} empty_rounds={empty_rounds}")
        if empty_rounds >= args.max_empty_rounds:
            print(f"[STOP] reached max_empty_rounds={args.max_empty_rounds}")
            break

    final_conn = _connect(canonical_db)
    final_state = _state(final_conn)
    final_conn.close()
    final_bench = _run_benchmark(backend_root, canonical_db, benchmark_path, report_dir, "final_benchmark")
    summary["final_state"] = final_state
    summary["final_benchmark"] = final_bench
    _write_json(report_dir / "expand_v5_summary.json", summary)

    print("\n" + "=" * 72)
    print("[FINAL RESULT]")
    print(f"  final_state: queryable={final_state['queryable']} family_coverage={final_state['family_coverage']} reaction_extracts={final_state['reaction_extracts']}")
    print(f"  final bench: top1={final_bench['top1_accuracy']:.4f} top3={final_bench['top3_accuracy']:.4f} violations={final_bench['disallow_top3_violations']}")
    print(f"  applied_total: {len(summary['applied_families'])}")
    print(f"  summary: {report_dir / 'expand_v5_summary.json'}")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
