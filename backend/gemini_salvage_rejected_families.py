from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

API_URL_TMPL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
DEFAULT_TOP_N = 2
DEFAULT_CANDIDATES_PER_FAMILY = 3
ALLOWED_SOURCES = {"gemini_auto_seed"}
LIKELY_FAMILY_COLS = (
    "reaction_family_name",
    "reaction_family_name_norm",
    "exact_pattern_family",
    "family_name",
    "pattern_family",
    "normalized_family",
    "family",
)

BUCHNER_CLUSTER = {
    "Claisen Condensation / Claisen Reaction",
    "Horner-Wadsworth-Emmons Olefination",
    "Krapcho Dealkoxycarbonylation",
    "Michael Addition Reaction",
    "Regitz Diazo Transfer",
}
BARTON_CLUSTER = {
    "Enyne Metathesis",
    "Finkelstein Reaction",
    "Hofmann-Loffler-Freytag Reaction",
    "Hunsdiecker Reaction",
    "Mitsunobu Reaction",
}

FAMILY_PROMPTS: dict[str, str] = {
    "Enyne Metathesis": "Textbook Ru-catalyzed enyne metathesis. Product must obviously arise from alkene+alkyne reorganization. Avoid halogen-rich or radical-looking substrates.",
    "Hofmann-Loffler-Freytag Reaction": "Textbook HLF reaction. N-haloamine or N-halosulfonamide gives remote intramolecular C-H amination to a cyclic amine. Avoid Barton-like motifs.",
    "Horner-Wadsworth-Emmons Olefination": "Distinctive phosphonate olefination. Phosphonate + aldehyde/ketone -> alkene. Prefer simple E-selective textbook example.",
    "Krapcho Dealkoxycarbonylation": "Textbook Krapcho example. Clear dealkoxycarbonylation of beta-keto ester or malonate-like substrate.",
    "Mitsunobu Reaction": "Textbook Mitsunobu substitution. Alcohol substitution under Mitsunobu conditions, ideally inversion on a simple secondary alcohol.",
    "Claisen Condensation / Claisen Reaction": "Textbook Claisen condensation. Ester enolate acyl substitution giving beta-keto ester or beta-dicarbonyl product.",
    "Michael Addition Reaction": "Textbook Michael addition. Soft/stabilized nucleophile does 1,4-addition to alpha,beta-unsaturated carbonyl.",
    "Regitz Diazo Transfer": "Textbook Regitz diazo transfer. Activated methylene compound -> alpha-diazo product. Avoid Buchner-like diazo/arene motifs.",
    "Finkelstein Reaction": "Classic SN2 halide exchange. Simple primary alkyl chloride/bromide -> iodide.",
    "Hunsdiecker Reaction": "Classic Hunsdiecker. Carboxylate-derived precursor -> one-carbon-shorter alkyl halide by decarboxylative halogenation.",
}

ROUND3_VARIANT_BRIEFS: dict[str, dict[str, str]] = {
    "Enyne Metathesis": {
        "C1": "Use an intramolecular ring-closing enyne metathesis that forms a six-membered diene ring from one tethered terminal alkene and one internal alkyne. Do not use halogens. Do not use simple linear cross-enyne examples.",
        "C2": "Use an intramolecular ring-closing enyne metathesis that forms a five-membered diene ring from a different tether length than C1. Keep substrate hydrocarbon-rich and neutral. No halogens, no heteroatom-rich motif.",
        "C3": "Use a strained or bicyclic-looking but still textbook intramolecular enyne metathesis outcome with an obvious diene product. Substrate must remain non-halogenated and non-radical-looking. Must not resemble C1 or C2 topology.",
    },
    "Hofmann-Loffler-Freytag Reaction": {
        "C1": "Use a classic N-halo sulfonamide or N-haloamine substrate that cyclizes to a pyrrolidine through remote 1,5-H abstraction. Keep substrate simple and non-bromocarboxylate. No Barton-like motif.",
        "C2": "Use a distinct piperidine-forming HLF example via 1,6-H abstraction from an N-halo precursor. Keep topology clearly different from C1. Avoid simple N-chloro dialkylamine only examples if possible.",
        "C3": "Use an HLF example with a clearly protected nitrogen precursor leading to cyclic amination, still textbook and remote-C-H-driven, but structurally different from C1/C2. Avoid decarboxylation, thiohydroxamate, peroxide, hv-style Barton overlap.",
    },
}

ANTI_COPY_RULES: dict[str, str] = {
    "Enyne Metathesis": "Do not reuse or lightly rewrite any previous candidate. The three candidates must have clearly different ring size or tether topology. Avoid simple terminal alkene + terminal alkyne linear pair. Prefer intramolecular ring-closing examples.",
    "Hofmann-Loffler-Freytag Reaction": "Do not output three near-identical N-chloroamine -> cyclic amine examples. The three candidates must differ in ring size or protecting-group topology. Keep remote intramolecular amination identity obvious.",
}


@dataclass
class FamilyTask:
    family_name: str
    changed_cases: int
    first_case: str
    first_top1: str
    cluster: str


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip().strip('"').strip("'")
        os.environ[key] = value


def placeholder_key(key: str) -> bool:
    low = key.strip().lower()
    return any(tok in low for tok in [
        "여기에_본인_api키",
        "your_api_key",
        "api_key_here",
        "placeholder",
        "example",
    ])


def pick_model(cli_model: str) -> str:
    if cli_model.strip():
        return cli_model.strip()
    env_model = (os.environ.get("GEMINI_MODEL") or "").strip()
    if env_model.lower().startswith("gemini-2.5"):
        return env_model
    return "gemini-2.5-pro"


def normalized(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip().lower()


def choose_variant(variants: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not variants:
        return None

    def score(v: dict[str, Any]) -> tuple[int, str]:
        cc = int(v.get("changed_case_count") or v.get("changed_cases") or 0)
        name = str(v.get("variant") or "")
        return (cc, name)

    return sorted(variants, key=score, reverse=True)[0]


def load_tasks(diag_summary_path: Path, top_n: int) -> list[FamilyTask]:
    data = json.loads(diag_summary_path.read_text(encoding="utf-8"))
    tasks: list[FamilyTask] = []
    for fam in data.get("families", []):
        family_name = fam.get("family_name") or fam.get("family") or ""
        variants = fam.get("variants", [])
        chosen = choose_variant(variants)
        changed_cases = int(((chosen or {}).get("changed_case_count")) or ((chosen or {}).get("changed_cases")) or 0)
        first_case = str(((chosen or {}).get("first_changed_case")) or ((chosen or {}).get("first_case")) or "")
        first_top1 = str(((chosen or {}).get("first_changed_top1")) or ((chosen or {}).get("first_top1")) or "")
        cluster = "buchner" if family_name in BUCHNER_CLUSTER else "barton" if family_name in BARTON_CLUSTER else "unknown"
        tasks.append(FamilyTask(family_name, changed_cases, first_case, first_top1, cluster))
    tasks.sort(key=lambda t: (-t.changed_cases, t.family_name))
    return tasks[:top_n]


def pick_existing_col(cols: list[str], cands: tuple[str, ...]) -> str | None:
    low = {c.lower(): c for c in cols}
    for c in cands:
        if c.lower() in low:
            return low[c.lower()]
    return None


def table_cols(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def family_extract_ids(conn: sqlite3.Connection, family_name: str) -> list[int]:
    cols = table_cols(conn, "reaction_extracts")
    fcol = pick_existing_col(cols, LIKELY_FAMILY_COLS)
    if not fcol:
        raise RuntimeError("reaction_extracts family column not found")
    rows = conn.execute(f"SELECT id FROM reaction_extracts WHERE TRIM({fcol})=? ORDER BY id", (family_name,)).fetchall()
    if rows:
        return [int(r[0]) for r in rows]
    rows = conn.execute(f"SELECT id, {fcol} FROM reaction_extracts ORDER BY id").fetchall()
    want = normalized(family_name)
    return [int(r[0]) for r in rows if normalized(str(r[1] or "")) == want]


def stage_examples(conn: sqlite3.Connection, extract_ids: list[int], limit: int = 2) -> list[dict[str, Any]]:
    if not extract_ids:
        return []
    conn.row_factory = sqlite3.Row
    marks = ",".join("?" * len(extract_ids))
    source_marks = ",".join("?" * len(ALLOWED_SOURCES))
    query = (
        f"SELECT extract_id, role, smiles FROM extract_molecules "
        f"WHERE extract_id IN ({marks}) AND structure_source IN ({source_marks}) "
        f"ORDER BY extract_id, id"
    )
    rows = conn.execute(query, tuple(extract_ids) + tuple(sorted(ALLOWED_SOURCES))).fetchall()
    return [dict(r) for r in rows][:limit]


def find_latest_diag_summary(backend_root: Path) -> Path:
    base = backend_root / "reports" / "v5_rejected_diagnose"
    candidates = sorted(base.glob("*/rejected_diagnosis_summary.json"))
    if not candidates:
        raise FileNotFoundError("No rejected_diagnosis_summary.json found")
    return candidates[-1]


def build_prompt(task: FamilyTask, examples: list[dict[str, Any]], candidate_id: str, relaxed: bool = False) -> str:
    cluster_note = (
        "Avoid Buchner-like diazo/arene insertion or generic activated-carbonyl overlap."
        if task.cluster == "buchner"
        else "Avoid Barton-like radical bromination, thiohydroxamate, peroxide/hv, or generic leaving-group activation motifs."
        if task.cluster == "barton"
        else "Avoid generic overlap with other named reactions."
    )
    family_specific = FAMILY_PROMPTS.get(task.family_name, "Create one textbook benchmark-safe example for this named reaction family.")
    variant_brief = ROUND3_VARIANT_BRIEFS.get(task.family_name, {}).get(candidate_id, "Use a topology clearly different from previous candidates.")
    anti_copy = ANTI_COPY_RULES.get(task.family_name, "The three candidates must be structurally distinct.")
    example_hint = ""
    if examples and not relaxed:
        pairs = [f"{e.get('role')}:{e.get('smiles')}" for e in examples if e.get('smiles')]
        if pairs:
            example_hint = " Stage hints (do not copy): " + " ; ".join(pairs[:2])
    if relaxed:
        prefix = "Keep it shorter. Give one small textbook-safe example only."
    else:
        prefix = "Need exactly one benchmark-safe canonical example."
    return (
        f"Family={task.family_name}. Candidate={candidate_id}. {prefix} "
        f"ChangedCases={task.changed_cases}. FirstHijackedCase={task.first_case}. WrongTop1={task.first_top1}. "
        f"{family_specific} {variant_brief} {anti_copy} {cluster_note}{example_hint} "
        "Return ONLY minified JSON: "
        '{"family":"...","candidate":{"candidate_id":"'+candidate_id+'","substrate_smiles":["..."],"product_smiles":["..."],"optional_reagent_smiles":["..."],"rationale":"...","collision_avoidance_note":"..."}} '
        "No markdown. rationale and collision_avoidance_note each under 70 chars."
    )


def _extract_candidate_text(data: dict[str, Any]) -> str:
    candidates = data.get("candidates") or []
    if not candidates:
        raise RuntimeError(f"Gemini returned no candidates: {json.dumps(data, ensure_ascii=False)[:1200]}")
    cand0 = candidates[0] or {}
    content = cand0.get("content") or {}
    parts = content.get("parts") or []
    texts: list[str] = []
    for part in parts:
        if isinstance(part, dict):
            txt = part.get("text")
            if isinstance(txt, str) and txt.strip():
                texts.append(txt.strip())
    if texts:
        return "\n".join(texts)
    finish_reason = cand0.get("finishReason") or data.get("promptFeedback", {}).get("blockReason") or "unknown"
    raise RuntimeError(f"Gemini response did not contain text parts (finish_reason={finish_reason})")


def _parse_response_json_text(text: str) -> dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.S)
    try:
        parsed = json.loads(text)
    except Exception:
        first = text.find("{")
        last = text.rfind("}")
        if first >= 0 and last > first:
            parsed = json.loads(text[first:last + 1])
        else:
            raise
    if isinstance(parsed, list):
        parsed = parsed[0]
    if not isinstance(parsed, dict):
        raise RuntimeError(f"Gemini parsed non-dict JSON: {type(parsed).__name__}")
    return parsed


def _payloads_for(model: str, prompt: str, temperature: float, max_tokens: int) -> list[dict[str, Any]]:
    capped_tokens = max(220, min(max_tokens, 320))
    payloads: list[dict[str, Any]] = []
    budgets = [16, 32] if model.startswith("gemini-2.5-pro") else [None]
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


def request_one_candidate(model: str, api_key: str, prompt: str, temperature: float, max_tokens: int) -> tuple[dict[str, Any], dict[str, Any]]:
    url = API_URL_TMPL.format(model=model, key=api_key)
    last_err: Exception | None = None
    last_data: dict[str, Any] | None = None
    for payload in _payloads_for(model, prompt, temperature, max_tokens):
        resp = requests.post(url, json=payload, timeout=180)
        resp.raise_for_status()
        data = resp.json()
        last_data = data
        try:
            text = _extract_candidate_text(data)
            parsed = _parse_response_json_text(text)
            return parsed, data
        except Exception as exc:
            last_err = exc
            continue
    raise RuntimeError(f"Failed to parse Gemini response after retries: {last_err}; raw={json.dumps(last_data or {}, ensure_ascii=False)[:2000]}")


def write_md_report(report_dir: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Gemini salvage summary",
        "",
        f"- model: `{summary['model']}`",
        f"- diag_summary: `{summary['diag_summary']}`",
        f"- total_families: {summary['total_families']}",
        f"- generated_at: `{summary['generated_at']}`",
        "",
    ]
    for fam in summary["families"]:
        lines.append(f"## {fam['family_name']}")
        lines.append("")
        lines.append(f"- changed_cases: {fam['changed_cases']}")
        lines.append(f"- first_case: `{fam['first_case']}`")
        lines.append(f"- first_top1: `{fam['first_top1']}`")
        lines.append(f"- cluster: `{fam['cluster']}`")
        if fam.get("error"):
            lines.append(f"- error: {fam['error']}")
        lines.append("")
        for cand in fam.get("candidates", []):
            lines.append(f"### {cand.get('candidate_id', 'candidate')}")
            lines.append("")
            lines.append(f"- substrate_smiles: `{json.dumps(cand.get('substrate_smiles', []), ensure_ascii=False)}`")
            lines.append(f"- product_smiles: `{json.dumps(cand.get('product_smiles', []), ensure_ascii=False)}`")
            lines.append(f"- optional_reagent_smiles: `{json.dumps(cand.get('optional_reagent_smiles', []), ensure_ascii=False)}`")
            lines.append(f"- rationale: {cand.get('rationale', '')}")
            lines.append(f"- collision_avoidance_note: {cand.get('collision_avoidance_note', '')}")
            lines.append("")
    (report_dir / "gemini_salvage_summary.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate Gemini salvage candidates for rejected CHEMLENS families")
    ap.add_argument("diag_summary", nargs="?", help="Path to rejected_diagnosis_summary.json. If omitted, latest one is auto-detected.")
    ap.add_argument("top_n", nargs="?", type=int, default=DEFAULT_TOP_N)
    ap.add_argument("--source-db", default=r"app\labint_v5_stage.db")
    ap.add_argument("--model", default="")
    ap.add_argument("--temperature", type=float, default=-1.0)
    ap.add_argument("--max-tokens", type=int, default=-1)
    ap.add_argument("--candidates-per-family", type=int, default=DEFAULT_CANDIDATES_PER_FAMILY)
    args = ap.parse_args()

    backend_root = Path.cwd()
    load_env_file(backend_root / ".env")

    api_key = (os.environ.get("GEMINI_API_KEY") or "").strip()
    if not api_key or placeholder_key(api_key):
        raise SystemExit("Valid GEMINI_API_KEY not found in backend .env")

    model = pick_model(args.model)
    try:
        temperature = args.temperature if args.temperature >= 0 else float(os.environ.get("GEMINI_TEMPERATURE", "0.1"))
    except Exception:
        temperature = 0.1
    try:
        max_tokens = args.max_tokens if args.max_tokens > 0 else int(os.environ.get("GEMINI_MAX_TOKENS", "512"))
    except Exception:
        max_tokens = 512

    diag_summary = Path(args.diag_summary).resolve() if args.diag_summary else find_latest_diag_summary(backend_root)
    source_db = (backend_root / Path(args.source_db)).resolve()

    report_dir = backend_root / "reports" / "gemini_rejected_salvage" / datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir.mkdir(parents=True, exist_ok=False)

    print("=" * 76)
    print("GEMINI SALVAGE FOR REJECTED FAMILIES")
    print("=" * 76)
    print(f"backend_root: {backend_root}")
    print(f"diag_summary:  {diag_summary}")
    print(f"source_db:     {source_db}")
    print(f"model:         {model}")
    print(f"top_n:         {args.top_n}")
    print(f"report dir:    {report_dir}")

    tasks = load_tasks(diag_summary, args.top_n)
    print(f"families loaded: {len(tasks)}")
    for t in tasks:
        print(f"  - {t.family_name} (changed_cases={t.changed_cases}, cluster={t.cluster}, first_case={t.first_case})")

    src = sqlite3.connect(str(source_db))
    try:
        families_out: list[dict[str, Any]] = []
        for task in tasks:
            print(f"\n[GEMINI] {task.family_name}")
            ex_ids = family_extract_ids(src, task.family_name)
            examples = stage_examples(src, ex_ids)
            safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", task.family_name)
            candidates: list[dict[str, Any]] = []
            prompt_files: list[str] = []
            raw_files: list[str] = []
            api_response_files: list[str] = []
            errors: list[str] = []

            for i in range(args.candidates_per_family):
                candidate_id = f"C{i+1}"
                success = False
                combined_errs: list[str] = []
                for relaxed in (False, True):
                    prompt = build_prompt(task, examples, candidate_id, relaxed=relaxed)
                    prompt_path = report_dir / f"{safe_name}_{candidate_id}_prompt{'_relaxed' if relaxed else ''}.txt"
                    prompt_path.write_text(prompt, encoding="utf-8")
                    prompt_files.append(prompt_path.name)
                    try:
                        raw, api_response = request_one_candidate(model, api_key, prompt, temperature, max_tokens)
                        api_path = report_dir / f"{safe_name}_{candidate_id}_api_response{'_relaxed' if relaxed else ''}.json"
                        api_path.write_text(json.dumps(api_response, ensure_ascii=False, indent=2), encoding="utf-8")
                        api_response_files.append(api_path.name)
                        raw_path = report_dir / f"{safe_name}_{candidate_id}_raw.json"
                        raw_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
                        raw_files.append(raw_path.name)
                        cand = raw.get("candidate") if isinstance(raw, dict) else None
                        if not isinstance(cand, dict):
                            raise RuntimeError(f"Gemini JSON missing 'candidate' object for {candidate_id}: {json.dumps(raw, ensure_ascii=False)[:1000]}")
                        if not cand.get("candidate_id"):
                            cand["candidate_id"] = candidate_id
                        candidates.append(cand)
                        success = True
                        break
                    except Exception as exc:
                        combined_errs.append(str(exc))
                        continue
                if not success:
                    err_path = report_dir / f"{safe_name}_{candidate_id}_raw.error.txt"
                    msg = " || ".join(combined_errs)
                    err_path.write_text(msg, encoding="utf-8")
                    raw_files.append(err_path.name)
                    errors.append(f"{candidate_id}: {msg}")
                    print(f"  {candidate_id} ERROR")

            print(f"  candidates: {len(candidates)}")
            families_out.append({
                "family_name": task.family_name,
                "changed_cases": task.changed_cases,
                "first_case": task.first_case,
                "first_top1": task.first_top1,
                "cluster": task.cluster,
                "stage_extract_ids": ex_ids,
                "stage_examples_used": examples,
                "candidates": candidates,
                "prompt_files": prompt_files,
                "raw_files": raw_files,
                "api_response_files": api_response_files,
                "error": " | ".join(errors),
            })

        summary = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "diag_summary": str(diag_summary),
            "source_db": str(source_db),
            "model": model,
            "total_families": len(families_out),
            "families": families_out,
        }
        (report_dir / "gemini_salvage_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        write_md_report(report_dir, summary)
    finally:
        src.close()

    print("\n" + "=" * 76)
    print("[DONE] gemini salvage generation finished")
    print(f"summary: {report_dir / 'gemini_salvage_summary.json'}")
    print("=" * 76)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
