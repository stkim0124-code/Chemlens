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
from typing import Any, Dict, Iterable, Tuple

import requests
from rdkit import Chem

SCRIPT_DIR = Path(__file__).resolve().parent


def load_env(path: Path) -> dict[str, str]:
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
        text = path.read_bytes().decode("utf-8", errors="ignore")
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    return conn


def table_cols(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def table_info_map(conn: sqlite3.Connection, table: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in conn.execute(f"PRAGMA table_info({table})").fetchall():
        out[row[1]] = {"notnull": bool(row[3]), "default": row[4], "type": (row[2] or "").upper()}
    return out


def next_id(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM {table}").fetchone()[0])


def safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_")[:80] or "item"


def strip_fences(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        parts = t.splitlines()
        if parts:
            parts = parts[1:]
        while parts and parts[-1].strip().startswith("```"):
            parts = parts[:-1]
        t = "\n".join(parts).strip()
    if t.lower().startswith("json\n"):
        t = t[5:].strip()
    return t


def extract_json_object(text: str) -> dict[str, Any]:
    t = strip_fences(text)
    try:
        return json.loads(t)
    except Exception:
        pass
    start = t.find("{")
    end = t.rfind("}")
    if start >= 0 and end > start:
        return json.loads(t[start:end+1])
    raise ValueError("Could not locate valid JSON object")


def extract_text_from_response(data: dict[str, Any]) -> str:
    candidates = data.get("candidates") or []
    cand0 = candidates[0] if candidates else {}
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
    finish_reason = cand0.get("finishReason") or "unknown"
    raise RuntimeError(f"Gemini response did not contain text parts (finish_reason={finish_reason})")


def request_gemini(api_key: str, model: str, prompt: str, max_tokens: int = 480, temperature: float = 0.2) -> Tuple[str, dict[str, Any]]:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    payloads = [
        {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "temperature": temperature,
                "maxOutputTokens": max_tokens,
                "thinkingConfig": {"thinkingBudget": 128},
            },
        },
        {
            "contents": [{"parts": [{"text": prompt + " Return plain JSON only."}]}],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_tokens,
                "thinkingConfig": {"thinkingBudget": 128},
            },
        },
    ]
    last_error: str | None = None
    for payload in payloads:
        try:
            resp = requests.post(url, json=payload, timeout=180)
            body = resp.text[:4000]
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}: {body}")
            data = resp.json()
            text = extract_text_from_response(data)
            return text, data
        except Exception as exc:
            last_error = str(exc)
    raise RuntimeError(last_error or "Gemini request failed")


def validate_smiles_list(smiles_list: Iterable[str]) -> list[str]:
    out: list[str] = []
    for s in smiles_list:
        ss = str(s or "").strip()
        if not ss:
            continue
        mol = Chem.MolFromSmiles(ss)
        if mol is None:
            raise ValueError(f"RDKit parse failed: {ss}")
        out.append(Chem.MolToSmiles(mol))
    return out


def run_benchmark(backend_root: Path, db_path: Path, benchmark_path: Path, report_dir: Path, prefix: str) -> dict[str, Any]:
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
    if isinstance(payload, dict) and "summary" in payload and isinstance(payload.get("summary"), dict):
        data = dict(payload["summary"])
        data["rows"] = payload.get("rows", [])
    else:
        data = payload if isinstance(payload, dict) else {"raw": payload}
    data["db_used"] = str(db_path)
    return data


def baseline_ok(bench: dict[str, Any]) -> bool:
    return float(bench.get("top1_accuracy") or 0.0) >= 0.9999 and float(bench.get("top3_accuracy") or 0.0) >= 0.9999 and int(bench.get("disallow_top3_violations") or 0) == 0


def build_extract_row(conn: sqlite3.Connection, family: str, family_norm: str, candidate: dict[str, Any], cfg: dict[str, Any], model: str) -> dict[str, Any]:
    info = table_info_map(conn, "reaction_extracts")
    row: dict[str, Any] = {}
    row["id"] = next_id(conn, "reaction_extracts")
    row["scheme_candidate_id"] = int(conn.execute("SELECT COALESCE(MIN(id),1) FROM scheme_candidates").fetchone()[0] or 1)
    row["reaction_family_name"] = family
    row["reaction_family_name_norm"] = family_norm
    row["extract_kind"] = "canonical_overview"
    row["transformation_text"] = cfg.get("transformation_text") or family
    row["reactants_text"] = "; ".join(candidate.get("substrate_smiles") or [])
    row["products_text"] = "; ".join(candidate.get("product_smiles") or [])
    row["reagents_text"] = candidate.get("reagents_text") or cfg.get("reagents_text") or ""
    row["conditions_text"] = candidate.get("conditions_text") or cfg.get("conditions_text") or ""
    row["notes_text"] = (candidate.get("rationale") or "") + (" | " + candidate.get("collision_avoidance_note") if candidate.get("collision_avoidance_note") else "")
    row["reactant_smiles"] = ".".join(candidate.get("substrate_smiles") or [])
    row["product_smiles"] = ".".join(candidate.get("product_smiles") or [])
    row["smiles_confidence"] = 0.95
    row["extraction_confidence"] = 0.8
    row["parse_status"] = "parsed"
    row["promote_decision"] = "candidate"
    row["extractor_model"] = model
    row["extractor_prompt_version"] = cfg.get("prompt_version") or "single_family_round1"
    row["extraction_raw_json"] = json.dumps(candidate, ensure_ascii=False)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row["created_at"] = now
    row["updated_at"] = now
    # fill any missing NOT NULL columns
    for col, meta in info.items():
        if col in row or col == "id":
            continue
        if meta["notnull"]:
            default = meta["default"]
            if default is not None:
                row[col] = default.strip("'") if isinstance(default, str) else default
            elif "INT" in meta["type"] or "REAL" in meta["type"]:
                row[col] = 0
            else:
                row[col] = ""
    return row


def build_molecule_rows(conn: sqlite3.Connection, extract_id: int, family: str, candidate: dict[str, Any], source_tag: str) -> list[dict[str, Any]]:
    info = table_info_map(conn, "extract_molecules")
    rows: list[dict[str, Any]] = []
    nid = next_id(conn, "extract_molecules")
    role_pairs: list[tuple[str, str]] = []
    for s in candidate.get("substrate_smiles") or []:
        role_pairs.append(("reactant", s))
    for s in candidate.get("product_smiles") or []:
        role_pairs.append(("product", s))
    for role, smiles in role_pairs:
        row: dict[str, Any] = {
            "id": nid,
            "extract_id": extract_id,
            "role": role,
            "smiles": smiles,
            "smiles_kind": "canonical",
            "quality_tier": 1,
            "reaction_family_name": family,
            "source_zip": source_tag,
            "page_no": 0,
            "queryable": 1,
            "note_text": candidate.get("rationale") or "",
            "normalized_text": family,
            "source_field": "gemini_rebuild_single_family",
            "structure_source": source_tag,
            "role_confidence": 0.95,
        }
        for col, meta in info.items():
            if col in row or col == "id":
                continue
            if meta["notnull"]:
                default = meta["default"]
                if default is not None:
                    row[col] = default.strip("'") if isinstance(default, str) else default
                elif "INT" in meta["type"] or "REAL" in meta["type"]:
                    row[col] = 0
                else:
                    row[col] = ""
        rows.append(row)
        nid += 1
    return rows


def insert_candidate(conn: sqlite3.Connection, family: str, family_norm: str, candidate: dict[str, Any], cfg: dict[str, Any], model: str) -> tuple[int, list[int]]:
    extract_row = build_extract_row(conn, family, family_norm, candidate, cfg, model)
    ex_cols = list(extract_row.keys())
    conn.execute(
        f"INSERT INTO reaction_extracts ({', '.join(ex_cols)}) VALUES ({', '.join(['?']*len(ex_cols))})",
        [extract_row[c] for c in ex_cols],
    )
    mol_rows = build_molecule_rows(conn, extract_row["id"], family, candidate, cfg.get("source_tag") or "gemini_rebuild_single_family_round1")
    for row in mol_rows:
        cols = list(row.keys())
        conn.execute(
            f"INSERT INTO extract_molecules ({', '.join(cols)}) VALUES ({', '.join(['?']*len(cols))})",
            [row[c] for c in cols],
        )
    return extract_row["id"], [r["id"] for r in mol_rows]


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate and benchmark-screen one family rebuild candidate via Gemini API.")
    ap.add_argument("--config", default=str(SCRIPT_DIR / "family_prompts" / "schwartz_hydrozirconation_round1.json"))
    ap.add_argument("--canonical", default=str(SCRIPT_DIR / "app" / "labint.db"))
    ap.add_argument("--benchmark", default=str(SCRIPT_DIR / "benchmark" / "named_reaction_benchmark_small.json"))
    ap.add_argument("--report-root", default=str(SCRIPT_DIR / "reports" / "gemini_single_family_rebuild"))
    ap.add_argument("--attempts", type=int, default=3)
    ap.add_argument("--model", default="gemini-2.5-pro")
    args = ap.parse_args()

    backend_root = SCRIPT_DIR
    cfg_path = Path(args.config).resolve()
    canonical = Path(args.canonical).resolve()
    benchmark = Path(args.benchmark).resolve()
    report_root = Path(args.report_root).resolve()
    report_dir = report_root / datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir.mkdir(parents=True, exist_ok=True)

    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    family = cfg["family"]
    family_norm = cfg.get("family_norm") or family.lower().strip()
    env = load_env(backend_root / ".env")
    api_key = (env.get("GEMINI_API_KEY") or os.environ.get("GEMINI_API_KEY") or "").strip()
    if not api_key:
        print("[ERROR] GEMINI_API_KEY not found in backend .env or environment")
        return 1

    print("=" * 72)
    print("GEMINI SINGLE-FAMILY REBUILD")
    print("=" * 72)
    print(f"family:      {family}")
    print(f"canonical:   {canonical}")
    print(f"benchmark:   {benchmark}")
    print(f"model:       {args.model}")
    print(f"attempts:    {args.attempts}")
    print(f"report dir:  {report_dir}")

    baseline = run_benchmark(backend_root, canonical, benchmark, report_dir, "benchmark_baseline")
    print(f"[baseline] top1={baseline['top1_accuracy']:.4f} top3={baseline['top3_accuracy']:.4f} violations={baseline['disallow_top3_violations']}")

    attempts: list[dict[str, Any]] = []
    accepted = None
    conn = connect(canonical)
    family_dir = report_dir / safe_name(family)
    family_dir.mkdir(parents=True, exist_ok=True)

    for idx in range(1, args.attempts + 1):
        prompt = cfg["prompt"]
        if idx > 1:
            prompt += "\nAdditional constraint: produce a structurally different substrate/product topology than prior attempts."
        (family_dir / f"attempt_{idx:02d}_prompt.txt").write_text(prompt, encoding="utf-8")
        try:
            raw, api_resp = request_gemini(api_key, args.model, prompt)
            (family_dir / f"attempt_{idx:02d}_api_response.json").write_text(json.dumps(api_resp, ensure_ascii=False, indent=2), encoding="utf-8")
            (family_dir / f"attempt_{idx:02d}_raw.txt").write_text(raw, encoding="utf-8")
            payload = extract_json_object(raw)
            candidate = payload.get("candidate") or payload
            candidate["substrate_smiles"] = validate_smiles_list(candidate.get("substrate_smiles") or [])
            candidate["product_smiles"] = validate_smiles_list(candidate.get("product_smiles") or [])
            candidate["optional_reagent_smiles"] = validate_smiles_list(candidate.get("optional_reagent_smiles") or [])
            if not candidate["substrate_smiles"] or not candidate["product_smiles"]:
                raise ValueError("substrate_smiles or product_smiles is empty after validation")
        except Exception as exc:
            msg = str(exc)
            (family_dir / f"attempt_{idx:02d}_error.txt").write_text(msg, encoding="utf-8")
            print(f"  attempt {idx}: ERROR {msg[:180]}")
            attempts.append({"attempt": idx, "status": "error", "error": msg})
            continue

        tmpdir = Path(tempfile.mkdtemp(prefix="single_family_rebuild_"))
        tmpdb = tmpdir / "labint.db"
        shutil.copy2(canonical, tmpdb)
        tmpconn = connect(tmpdb)
        try:
            extract_id, mol_ids = insert_candidate(tmpconn, family, family_norm, candidate, cfg, args.model)
            tmpconn.commit()
            bench = run_benchmark(backend_root, tmpdb, benchmark, family_dir, f"attempt_{idx:02d}_bench")
        except Exception as exc:
            msg = str(exc)
            (family_dir / f"attempt_{idx:02d}_insert_or_bench_error.txt").write_text(msg, encoding="utf-8")
            print(f"  attempt {idx}: INSERT/BENCH ERROR {msg[:180]}")
            attempts.append({"attempt": idx, "status": "insert_or_bench_error", "error": msg, "candidate": candidate})
            try:
                tmpconn.close()
            except Exception:
                pass
            shutil.rmtree(tmpdir, ignore_errors=True)
            continue
        finally:
            try:
                tmpconn.close()
            except Exception:
                pass
            shutil.rmtree(tmpdir, ignore_errors=True)

        passed = baseline_ok(bench)
        print(f"  attempt {idx}: top1={bench['top1_accuracy']:.4f} top3={bench['top3_accuracy']:.4f} violations={bench['disallow_top3_violations']} {'PASS' if passed else 'FAIL'}")
        rec = {
            "attempt": idx,
            "status": "pass" if passed else "fail",
            "candidate": candidate,
            "benchmark": bench,
        }
        attempts.append(rec)
        if passed and accepted is None:
            accepted = rec

    summary = {
        "family": family,
        "family_norm": family_norm,
        "mode": "DRY-RUN",
        "canonical": str(canonical),
        "benchmark": str(benchmark),
        "model": args.model,
        "config": str(cfg_path),
        "baseline": baseline,
        "attempts": attempts,
        "accepted": accepted,
        "report_dir": str(report_dir),
    }
    out = report_dir / "gemini_single_family_rebuild_summary.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print("=" * 72)
    print(f"[DONE] summary: {out}")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
