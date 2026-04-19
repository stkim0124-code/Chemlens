from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

SCRIPT_DIR = Path(__file__).resolve().parent

TARGETS: list[dict[str, Any]] = [
    {
        "family": "Claisen Condensation / Claisen Reaction",
        "cluster": "buchner",
        "changed_cases": 1,
        "forbid": ["diazo_arene_combo", "ring_expansion"],
        "guidance": "Propose a textbook Claisen condensation core pair that is unambiguously ester enolate acyl substitution, not diazo chemistry, not ring expansion, and not carbene/arene insertion.",
    },
    {
        "family": "Horner-Wadsworth-Emmons Olefination",
        "cluster": "buchner",
        "changed_cases": 2,
        "forbid": ["diazo_arene_combo", "ring_expansion"],
        "guidance": "Propose a simple phosphonate-stabilized olefination example that clearly forms an alkene from aldehyde plus phosphonate, with no diazo or ring-expansion motifs.",
    },
    {
        "family": "Krapcho Dealkoxycarbonylation",
        "cluster": "buchner",
        "changed_cases": 2,
        "forbid": ["diazo_arene_combo", "ring_expansion"],
        "guidance": "Propose a minimal textbook Krapcho dealkoxycarbonylation example where a beta-keto ester or malonate-type substrate loses an alkoxycarbonyl group. Avoid diazo or arene-carbene motifs entirely.",
    },
    {
        "family": "Michael Addition Reaction",
        "cluster": "buchner",
        "changed_cases": 1,
        "forbid": ["diazo_arene_combo", "ring_expansion"],
        "guidance": "Propose a simple Michael addition example with a clear conjugate acceptor and nucleophile, producing a 1,4-addition product. Avoid diazo, ring expansion, and arene-carbene features.",
    },
    {
        "family": "Regitz Diazo Transfer",
        "cluster": "buchner",
        "changed_cases": 1,
        "forbid": ["diazo_arene_combo", "ring_expansion"],
        "guidance": "Propose a Regitz diazo transfer example where an active methylene compound becomes a diazo derivative, but do not use any aryl-carbene insertion or Buchner-like ring-expansion topology.",
    },
    {
        "family": "Enyne Metathesis",
        "cluster": "barton",
        "changed_cases": 2,
        "forbid": ["decarboxylation", "deoxygenation"],
        "guidance": "Propose a concise ring-closing enyne metathesis example with one enyne substrate and one cyclized diene product. Avoid any carboxylate, decarboxylation, deoxygenation, thiohydroxamate, Barton or McCombie-like motifs.",
    },
    {
        "family": "Hofmann-Loffler-Freytag Reaction",
        "cluster": "barton",
        "changed_cases": 2,
        "forbid": ["decarboxylation", "deoxygenation"],
        "guidance": "Propose a textbook HLF intramolecular C-H amination example from a simple N-haloamine to a cyclic amine. Avoid carboxylates, decarboxylation, deoxygenation, thioesters, or Barton-like radical precursors.",
    },
    {
        "family": "Mitsunobu Reaction",
        "cluster": "barton",
        "changed_cases": 2,
        "forbid": ["decarboxylation", "deoxygenation"],
        "guidance": "Propose a simple Mitsunobu substitution/inversion example from an alcohol plus acidic nucleophile to substituted product. Avoid any decarboxylation or deoxygenation-like topology.",
    },
]

ROLE_ORDER = ["reactant", "product", "reagent", "intermediate", "catalyst", "solvent", "unknown"]


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    return conn


def _table_cols(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _table_notnull_defaults(conn: sqlite3.Connection, table: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in conn.execute(f"PRAGMA table_info({table})").fetchall():
        out[row[1]] = {"notnull": bool(row[3]), "default": row[4], "type": (row[2] or "").upper()}
    return out


def _load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    text = None
    for enc in ("utf-8", "utf-8-sig", "cp949", "latin-1"):
        try:
            text = path.read_text(encoding=enc)
            break
        except Exception:
            pass
    if text is None:
        text = path.read_bytes().decode("ascii", errors="ignore")
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def _safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_")[:80] or "item"


def _strip_fences(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        parts = t.split("\n")
        if parts:
            parts = parts[1:]
        while parts and parts[-1].strip().startswith("```"):
            parts = parts[:-1]
        t = "\n".join(parts).strip()
    return t


def _extract_json_object(text: str) -> dict[str, Any]:
    t = _strip_fences(text)
    try:
        return json.loads(t)
    except Exception:
        pass
    start = t.find("{")
    end = t.rfind("}")
    if start >= 0 and end > start:
        return json.loads(t[start:end+1])
    raise ValueError("Could not locate valid JSON object")


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
    capped_tokens = max(256, min(max_tokens, 640))
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


def _request_gemini(api_key: str, model: str, prompt: str, max_tokens: int = 512, temperature: float = 0.2, thinking_budget: int = 64) -> Tuple[str, dict[str, Any]]:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    last_error: Exception | None = None
    for payload in _payloads_for(model, prompt, temperature, max_tokens):
        try:
            resp = requests.post(url, json=payload, timeout=180)
            body = resp.text[:4000]
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}: {body}")
            data = resp.json()
            text = _extract_candidate_text(data)
            return text, data
        except Exception as exc:
            last_error = exc
            continue
    raise RuntimeError(f"Gemini request failed after retries: {last_error}")


def _validate_smiles_list(smiles_list: Iterable[str]) -> list[str]:
    from rdkit import Chem

    cleaned: list[str] = []
    for s in smiles_list:
        if not s or not str(s).strip():
            continue
        mol = Chem.MolFromSmiles(str(s).strip())
        if mol is None:
            raise ValueError(f"RDKit parse failed: {s}")
        cleaned.append(Chem.MolToSmiles(mol))
    return cleaned


def _state(conn: sqlite3.Connection) -> dict[str, int]:
    q = conn.execute("SELECT COUNT(*) FROM extract_molecules WHERE queryable=1").fetchone()[0]
    fc = conn.execute("SELECT COUNT(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1").fetchone()[0]
    re_cnt = conn.execute("SELECT COUNT(*) FROM reaction_extracts").fetchone()[0]
    em_cnt = conn.execute("SELECT COUNT(*) FROM extract_molecules").fetchone()[0]
    return {"queryable": q, "family_coverage": fc, "reaction_extracts": re_cnt, "extract_molecules_total": em_cnt}


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
    # run_named_reaction_benchmark_small.py writes {"summary": {...}, "rows": [...]}
    # but some older helpers expected the summary fields at the root. Normalize here.
    if isinstance(payload, dict) and "summary" in payload and isinstance(payload.get("summary"), dict):
        data = dict(payload["summary"])
        data["rows"] = payload.get("rows", [])
    else:
        data = payload if isinstance(payload, dict) else {"raw": payload}
    data["db_used"] = str(db_path)
    return data


def _common_non_id_cols(src_cols: list[str], dst_cols: list[str]) -> list[str]:
    return [c for c in src_cols if c in dst_cols and c not in {"id"}]


def _next_id(conn: sqlite3.Connection, table: str) -> int:
    row = conn.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM {table}").fetchone()
    return int(row[0])


def _fetch_stage_extract(stage_conn: sqlite3.Connection, family: str) -> Tuple[sqlite3.Row, list[sqlite3.Row]]:
    re_row = stage_conn.execute(
        """
        SELECT re.*
        FROM reaction_extracts re
        JOIN extract_molecules em ON em.extract_id = re.id
        WHERE re.reaction_family_name = ? AND em.queryable=1
        ORDER BY re.id LIMIT 1
        """,
        (family,),
    ).fetchone()
    if re_row is None:
        raise RuntimeError(f"Stage extract not found for {family}")
    em_rows = stage_conn.execute(
        "SELECT * FROM extract_molecules WHERE extract_id=? AND queryable=1 ORDER BY CASE role WHEN 'reactant' THEN 1 WHEN 'product' THEN 2 WHEN 'reagent' THEN 3 ELSE 99 END, id",
        (re_row["id"],),
    ).fetchall()
    if not em_rows:
        raise RuntimeError(f"No queryable stage molecules for {family}")
    return re_row, em_rows


def _prepare_extract_insert_values(src_row: sqlite3.Row, dst_conn: sqlite3.Connection) -> dict[str, Any]:
    dst_cols = _table_cols(dst_conn, "reaction_extracts")
    src_cols = list(src_row.keys())
    cols = _common_non_id_cols(src_cols, dst_cols)
    vals = {c: src_row[c] for c in cols}
    info = _table_notnull_defaults(dst_conn, "reaction_extracts")
    if "scheme_candidate_id" in dst_cols and (vals.get("scheme_candidate_id") in (None, "")):
        fallback = dst_conn.execute("SELECT COALESCE(MIN(id),1) FROM scheme_candidates").fetchone()[0]
        vals["scheme_candidate_id"] = fallback or 1
    for c, meta in info.items():
        if c == "id":
            continue
        if meta["notnull"] and c not in vals:
            default = meta["default"]
            if default is not None:
                vals[c] = default.strip("'") if isinstance(default, str) else default
            elif "INT" in meta["type"]:
                vals[c] = 0
            else:
                vals[c] = ""
    return vals


def _template_for_role(stage_molecules: list[sqlite3.Row], role: str) -> sqlite3.Row:
    for row in stage_molecules:
        if (row.get("role") if hasattr(row, 'get') else row["role"]) == role:
            return row
    return stage_molecules[0]


def _row_get(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except Exception:
        return default


def _insert_candidate_pair(conn: sqlite3.Connection, stage_re: sqlite3.Row, stage_mols: list[sqlite3.Row], candidate: dict[str, Any], structure_source: str) -> tuple[int, list[int]]:
    re_vals = _prepare_extract_insert_values(stage_re, conn)
    new_extract_id = _next_id(conn, "reaction_extracts")
    re_vals["id"] = new_extract_id
    re_cols = list(re_vals.keys())
    conn.execute(
        f"INSERT INTO reaction_extracts ({', '.join(re_cols)}) VALUES ({', '.join(['?']*len(re_cols))})",
        [re_vals[c] for c in re_cols],
    )

    dst_cols = _table_cols(conn, "extract_molecules")
    info = _table_notnull_defaults(conn, "extract_molecules")
    new_ids: list[int] = []
    next_em_id = _next_id(conn, "extract_molecules")

    role_to_smiles: list[tuple[str, str]] = []
    for s in candidate.get("substrate_smiles", []) or []:
        role_to_smiles.append(("reactant", s))
    for s in candidate.get("product_smiles", []) or []:
        role_to_smiles.append(("product", s))
    for s in candidate.get("optional_reagent_smiles", []) or []:
        role_to_smiles.append(("reagent", s))

    for role, smiles in role_to_smiles:
        template = _template_for_role(stage_mols, role)
        vals: dict[str, Any] = {}
        for c in dst_cols:
            if c == "id":
                continue
            if c == "extract_id":
                vals[c] = new_extract_id
            elif c == "role":
                vals[c] = role
            elif c == "smiles":
                vals[c] = smiles
            elif c == "canonical_smiles":
                vals[c] = smiles
            elif c == "queryable":
                vals[c] = 1
            elif c == "structure_source":
                vals[c] = structure_source
            else:
                if c in template.keys():
                    vals[c] = template[c]
        for c, meta in info.items():
            if c == "id":
                continue
            if meta["notnull"] and (c not in vals or vals[c] is None):
                default = meta["default"]
                if default is not None:
                    vals[c] = default.strip("'") if isinstance(default, str) else default
                elif c == "role":
                    vals[c] = role
                elif "INT" in meta["type"]:
                    vals[c] = 0
                else:
                    vals[c] = ""
        vals["id"] = next_em_id
        cols = list(vals.keys())
        conn.execute(
            f"INSERT INTO extract_molecules ({', '.join(cols)}) VALUES ({', '.join(['?']*len(cols))})",
            [vals[c] for c in cols],
        )
        new_ids.append(next_em_id)
        next_em_id += 1
    return new_extract_id, new_ids


def _build_prompt(target: dict[str, Any], stage_re: sqlite3.Row, stage_mols: list[sqlite3.Row], attempt_idx: int) -> str:
    role_lines = []
    for row in stage_mols:
        role_lines.append(f"- role={row['role']} smiles={row['smiles']}")
    extra = ""
    if attempt_idx > 1:
        extra = "Return a structurally different topology than any prior attempt. Avoid repeating the same substrate skeleton."
    return (
        f"Generate exactly one benchmark-safe canonical example for the named reaction family '{target['family']}'.\n"
        f"Cluster risk: {target['cluster']} ; forbid signals: {', '.join(target['forbid'])}.\n"
        f"Current rejected stage seed to avoid copying:\n" + "\n".join(role_lines) + "\n"
        f"Goal: {target['guidance']}\n"
        f"Rules:\n"
        f"- keep it textbook and minimal\n"
        f"- avoid the forbidden signals above\n"
        f"- do not repeat the current rejected stage topology\n"
        f"- provide valid organic SMILES only; no catalyst metals in SMILES unless absolutely necessary\n"
        f"- keep reagent list short or empty if not essential\n"
        f"{extra}\n"
        f"Return JSON only with this schema:\n"
        f"{{\"substrate_smiles\":[\"...\"],\"product_smiles\":[\"...\"],\"optional_reagent_smiles\":[\"...\"]}}"
    )


def _baseline_ok(bench: dict[str, Any]) -> bool:
    return float(bench.get("top1_accuracy") or 0.0) >= 0.9999 and float(bench.get("top3_accuracy") or 0.0) >= 0.9999 and int(bench.get("disallow_top3_violations") or 0) == 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Use Gemini to rebuild rejected 8 seeds and screen them with benchmark guard.")
    ap.add_argument("--canonical", default=str(SCRIPT_DIR / "app" / "labint.db"))
    ap.add_argument("--stage", default=str(SCRIPT_DIR / "app" / "labint_v5_stage.db"))
    ap.add_argument("--benchmark", default=str(SCRIPT_DIR / "benchmark" / "named_reaction_benchmark_small.json"))
    ap.add_argument("--report-dir", default=str(SCRIPT_DIR / "reports" / "gemini_rebuild_rejected8"))
    ap.add_argument("--max-attempts", type=int, default=3)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--model", default="gemini-2.5-pro")
    args = ap.parse_args()

    backend_root = SCRIPT_DIR
    canonical = Path(args.canonical).resolve()
    stage = Path(args.stage).resolve()
    benchmark = Path(args.benchmark).resolve()
    report_root = Path(args.report_dir).resolve()
    report_dir = report_root / datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir.mkdir(parents=True, exist_ok=True)

    env = _load_env(backend_root / ".env")
    api_key = (env.get("GEMINI_API_KEY") or os.environ.get("GEMINI_API_KEY") or "").strip()
    if not api_key:
        print("[ERROR] GEMINI_API_KEY not found in .env or environment")
        return 1

    print("=" * 72)
    print("GEMINI REBUILD REJECTED 8 SEEDS")
    print("=" * 72)
    print(f"mode:        {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"canonical:   {canonical}")
    print(f"stage:       {stage}")
    print(f"benchmark:   {benchmark}")
    print(f"model:       {args.model}")
    print(f"max_attempts:{args.max_attempts}")
    print(f"report dir:  {report_dir}")

    canonical_conn = _connect(canonical)
    stage_conn = _connect(stage)
    baseline = _run_benchmark(backend_root, canonical, benchmark, report_dir, "benchmark_baseline")
    print(f"[baseline] top1={baseline['top1_accuracy']:.4f} top3={baseline['top3_accuracy']:.4f} violations={baseline['disallow_top3_violations']}")

    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    for target in TARGETS:
        family = target["family"]
        fam_dir = report_dir / _safe_name(family)
        fam_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n[{family}]")
        try:
            stage_re, stage_mols = _fetch_stage_extract(stage_conn, family)
        except Exception as e:
            print(f"  stage fetch error: {e}")
            rejected.append({"family": family, "status": "stage_fetch_error", "error": str(e)})
            continue

        family_success = None
        family_attempts: list[dict[str, Any]] = []
        for attempt_idx in range(1, args.max_attempts + 1):
            prompt = _build_prompt(target, stage_re, stage_mols, attempt_idx)
            (fam_dir / f"attempt_{attempt_idx:02d}_prompt.txt").write_text(prompt, encoding="utf-8")
            try:
                raw_text, api_resp = _request_gemini(api_key, args.model, prompt)
                (fam_dir / f"attempt_{attempt_idx:02d}_api_response.json").write_text(json.dumps(api_resp, ensure_ascii=False, indent=2), encoding="utf-8")
                (fam_dir / f"attempt_{attempt_idx:02d}_raw.txt").write_text(raw_text, encoding="utf-8")
                cand = _extract_json_object(raw_text)
                cand["substrate_smiles"] = _validate_smiles_list(cand.get("substrate_smiles") or [])
                cand["product_smiles"] = _validate_smiles_list(cand.get("product_smiles") or [])
                cand["optional_reagent_smiles"] = _validate_smiles_list(cand.get("optional_reagent_smiles") or [])
            except Exception as e:
                msg = str(e)
                (fam_dir / f"attempt_{attempt_idx:02d}_error.txt").write_text(msg, encoding="utf-8")
                print(f"  attempt {attempt_idx}: ERROR {msg[:180]}")
                family_attempts.append({"attempt": attempt_idx, "status": "error", "error": msg})
                continue

            tmpdir = Path(tempfile.mkdtemp(prefix="rebuild_rejected8_"))
            tmpdb = tmpdir / "labint.db"
            shutil.copy2(canonical, tmpdb)
            tmpconn = _connect(tmpdb)
            try:
                _insert_candidate_pair(tmpconn, stage_re, stage_mols, cand, "gemini_salvage_seed")
                tmpconn.commit()
                bench = _run_benchmark(backend_root, tmpdb, benchmark, fam_dir, f"attempt_{attempt_idx:02d}_bench")
            except Exception as e:
                msg = str(e)
                (fam_dir / f"attempt_{attempt_idx:02d}_insert_or_bench_error.txt").write_text(msg, encoding="utf-8")
                print(f"  attempt {attempt_idx}: INSERT/BENCH ERROR {msg[:180]}")
                family_attempts.append({"attempt": attempt_idx, "status": "insert_or_bench_error", "error": msg, "candidate": cand})
                tmpconn.close()
                shutil.rmtree(tmpdir, ignore_errors=True)
                continue
            finally:
                try:
                    tmpconn.close()
                except Exception:
                    pass
                shutil.rmtree(tmpdir, ignore_errors=True)

            passed = _baseline_ok(bench)
            print(f"  attempt {attempt_idx}: top1={bench['top1_accuracy']:.4f} top3={bench['top3_accuracy']:.4f} violations={bench['disallow_top3_violations']} {'PASS' if passed else 'FAIL'}")
            rec = {"attempt": attempt_idx, "status": "pass" if passed else "fail", "candidate": cand, "benchmark": bench}
            family_attempts.append(rec)
            if passed:
                family_success = rec
                break

        if family_success:
            accepted.append({"family": family, "cluster": target["cluster"], "stage_extract_id": int(stage_re["id"]), **family_success})
        else:
            rejected.append({"family": family, "cluster": target["cluster"], "stage_extract_id": int(stage_re["id"]), "attempts": family_attempts})

    applied = []
    if args.apply and accepted:
        backup = canonical.with_name(f"labint.backup_before_gemini_rebuild_rejected8_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
        shutil.copy2(canonical, backup)
        print(f"\n[backup] {backup.name}")
        conn = _connect(canonical)
        for item in accepted:
            family = item["family"]
            stage_re, stage_mols = _fetch_stage_extract(stage_conn, family)
            _insert_candidate_pair(conn, stage_re, stage_mols, item["candidate"], "gemini_salvage_seed")
            applied.append(family)
        conn.commit()
        conn.close()
        final_bench = _run_benchmark(backend_root, canonical, benchmark, report_dir, "benchmark_after_apply")
        print(f"\n[after apply] top1={final_bench['top1_accuracy']:.4f} top3={final_bench['top3_accuracy']:.4f} violations={final_bench['disallow_top3_violations']}")
    else:
        final_bench = None

    summary = {
        "mode": "APPLY" if args.apply else "DRY-RUN",
        "canonical": str(canonical),
        "stage": str(stage),
        "benchmark": str(benchmark),
        "baseline": baseline,
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
        "accepted": accepted,
        "rejected": rejected,
        "applied_families": applied,
        "final_benchmark": final_bench,
    }
    (report_dir / "gemini_rebuild_rejected8_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n" + "=" * 72)
    print("[RESULT]")
    print(f"  accepted: {len(accepted)}")
    print(f"  rejected: {len(rejected)}")
    if applied:
        print(f"  applied:  {', '.join(applied)}")
    print(f"  summary:  {report_dir / 'gemini_rebuild_rejected8_summary.json'}")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
