import json, os, sqlite3, subprocess, sys, shutil
from pathlib import Path
from datetime import datetime

TARGET_SHORT_FAMILIES = [
    "Diels-Alder Cycloaddition",
    "Fries Rearrangement",
    "Houben-Hoesch Reaction",
    "Finkelstein Reaction",
    "Hunsdiecker Reaction",
    "Swern Oxidation",
    "Wolff-Kishner Reduction",
    "Pinner Reaction",
    "Ritter Reaction",
    "Tsuji-Wilkinson Decarbonylation Reaction",
    "Pfitzner-Moffatt Oxidation",
    "Schwartz Hydrozirconation",
]

ALIAS_PAIRS = [
    ("Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement", "Fries Rearrangement"),
    ("Hofmann-Löffler-Freytag Reaction (Remote Functionalization)", "Hofmann-Loffler-Freytag Reaction"),
    ("Houben-Hoesch Reaction/Synthesis", "Houben-Hoesch Reaction"),
]


def ts():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def cols(conn, table):
    try:
        return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    except Exception:
        return []


def has_table(conn, table):
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


def family_col(conn, table):
    candidates = ["reaction_family_name", "family_name", "family"]
    present = cols(conn, table)
    for c in candidates:
        if c in present:
            return c
    return None


def choose(conn, table, names):
    present = cols(conn, table)
    for n in names:
        if n in present:
            return n
    return None


def benchmark(backend_root: Path, db_path: Path, report_dir: Path):
    runner = backend_root / "benchmark" / "run_named_reaction_benchmark_small.py"
    benchmark_json = backend_root / "benchmark" / "named_reaction_benchmark_small.json"
    out_json = report_dir / "benchmark_current.json"
    out_csv = report_dir / "benchmark_current.csv"
    out_md = report_dir / "benchmark_current.md"
    cmd = [
        sys.executable,
        str(runner),
        "--db", str(db_path),
        "--benchmark", str(benchmark_json),
        "--json-out", str(out_json),
        "--csv-out", str(out_csv),
        "--report-md", str(out_md),
    ]
    cp = subprocess.run(cmd, cwd=str(backend_root), capture_output=True, text=True)
    if cp.returncode != 0:
        return {"ok": False, "returncode": cp.returncode, "stdout": cp.stdout[-4000:], "stderr": cp.stderr[-4000:]}
    data = json.loads(out_json.read_text(encoding="utf-8"))
    if "summary" in data and isinstance(data["summary"], dict):
        data = data["summary"]
    data["ok"] = True
    data["json_out"] = str(out_json)
    data["csv_out"] = str(out_csv)
    data["md_out"] = str(out_md)
    return data


def family_extract_count(conn, fam):
    if not has_table(conn, "reaction_extracts"):
        return 0
    fc = family_col(conn, "reaction_extracts")
    if not fc:
        return 0
    return int(conn.execute(f"SELECT COUNT(*) FROM reaction_extracts WHERE {fc}=?", (fam,)).fetchone()[0])


def family_queryable_mol_count(conn, fam):
    if not has_table(conn, "extract_molecules"):
        return 0
    fc = family_col(conn, "extract_molecules")
    qc = choose(conn, "extract_molecules", ["queryable", "is_queryable"])
    if not fc:
        return 0
    sql = f"SELECT COUNT(*) FROM extract_molecules WHERE {fc}=?"
    if qc:
        sql += f" AND COALESCE({qc},0)=1"
    return int(conn.execute(sql, (fam,)).fetchone()[0])


def family_pattern_row(conn, fam):
    if not has_table(conn, "reaction_family_patterns"):
        return None
    c = family_col(conn, "reaction_family_patterns")
    if not c:
        c = choose(conn, "reaction_family_patterns", ["family_name", "family"])
    if not c:
        return None
    conn.row_factory = sqlite3.Row
    row = conn.execute(f"SELECT * FROM reaction_family_patterns WHERE {c}=?", (fam,)).fetchone()
    conn.row_factory = None
    return dict(row) if row else None


def alias_pair_status(conn, long_alias, short_name):
    syn_contains = False
    long_shell = family_pattern_row(conn, long_alias)
    short_row = family_pattern_row(conn, short_name)
    if short_row:
        syn = str(short_row.get("synonym_names") or "")
        syn_contains = long_alias in syn
    abbr_match = False
    if has_table(conn, "abbreviation_aliases"):
        rows = conn.execute("SELECT * FROM abbreviation_aliases").fetchall()
        for row in rows:
            vals = [str(v) for v in row if v is not None]
            text = " | ".join(vals)
            if long_alias in text and short_name in text:
                abbr_match = True
                break
    return {
        "long_alias": long_alias,
        "short_name": short_name,
        "short_synonym_contains_long": syn_contains,
        "abbreviation_alias_pair_found": abbr_match,
        "long_shell_present": long_shell is not None,
    }


def collect_apply_summaries(backend_root: Path):
    out = []
    reports = backend_root / "reports"
    if not reports.exists():
        return out
    patterns = [
        reports / "gemini_salvage_apply",
        reports / "gemini_single_family_apply",
    ]
    for base in patterns:
        if not base.exists():
            continue
        for p in sorted(base.glob("*/*summary.json")):
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
            fam = data.get("family") or data.get("selected_family")
            if not fam:
                # older multi-family apply summary
                fams = data.get("selected_families") or []
                if fams:
                    fam = ", ".join(sorted(set(fams)))
            out.append({"path": str(p), "family": fam, "data": data})
    return out


def main():
    backend_root = Path(__file__).resolve().parent
    canonical = backend_root / "app" / "labint.db"
    work_db = backend_root / "app" / "labint_round9_bridge_work.db"
    stage_db = backend_root / "app" / "labint_v5_stage.db"
    report_dir = backend_root / "reports" / "final_state_verification" / ts()
    report_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 72)
    print("FINAL STATE VERIFICATION")
    print("=" * 72)
    print(f"backend_root: {backend_root}")
    print(f"canonical:    {canonical}")
    print(f"work_db:      {work_db}")
    print(f"stage_db:     {stage_db}")
    print(f"report_dir:   {report_dir}")

    con = sqlite3.connect(str(canonical))
    summary = {
        "canonical_db": str(canonical),
        "work_db": str(work_db),
        "stage_db": str(stage_db),
        "report_dir": str(report_dir),
        "benchmark": benchmark(backend_root, canonical, report_dir),
        "family_status": [],
        "alias_status": [],
        "apply_summaries_found": collect_apply_summaries(backend_root),
    }

    for fam in TARGET_SHORT_FAMILIES:
        row = family_pattern_row(con, fam)
        status = {
            "family": fam,
            "pattern_present": row is not None,
            "reaction_extract_count": family_extract_count(con, fam),
            "queryable_molecule_count": family_queryable_mol_count(con, fam),
            "enabled_for_reaction_query": row.get("enabled_for_reaction_query") if row else None,
            "enabled_for_single_smiles": row.get("enabled_for_single_smiles") if row else None,
        }
        summary["family_status"].append(status)
        print(f"[FAMILY] {fam} | pattern={status['pattern_present']} extracts={status['reaction_extract_count']} queryable_mols={status['queryable_molecule_count']}")

    for long_alias, short_name in ALIAS_PAIRS:
        st = alias_pair_status(con, long_alias, short_name)
        summary["alias_status"].append(st)
        print(f"[ALIAS] {long_alias} -> {short_name} | syn={st['short_synonym_contains_long']} abbr={st['abbreviation_alias_pair_found']} long_shell={st['long_shell_present']}")

    con.close()

    # high-level checks
    resolved = []
    unresolved = []
    for s in summary["family_status"]:
        if s["reaction_extract_count"] > 0 and s["queryable_molecule_count"] > 0:
            resolved.append(s["family"])
        else:
            unresolved.append(s["family"])
    summary["resolved_short_families"] = resolved
    summary["possibly_unresolved_short_families"] = unresolved

    ok_alias = []
    bad_alias = []
    for a in summary["alias_status"]:
        if a["short_synonym_contains_long"] or a["abbreviation_alias_pair_found"]:
            ok_alias.append(a["long_alias"])
        else:
            bad_alias.append(a["long_alias"])
    summary["alias_pairs_ok"] = ok_alias
    summary["alias_pairs_needing_review"] = bad_alias

    json_path = report_dir / "final_state_verification_summary.json"
    md_path = report_dir / "final_state_verification_summary.md"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = []
    lines.append("# Final State Verification\n")
    lines.append(f"- canonical: `{canonical}`")
    lines.append(f"- benchmark ok: `{summary['benchmark'].get('ok')}`")
    if summary['benchmark'].get('ok'):
        lines.append(f"- top1: `{summary['benchmark'].get('top1_accuracy')}`")
        lines.append(f"- top3: `{summary['benchmark'].get('top3_accuracy')}`")
        lines.append(f"- violations: `{summary['benchmark'].get('disallow_top3_violations')}`")
    lines.append("\n## Short-form family status\n")
    for s in summary["family_status"]:
        lines.append(f"- {s['family']}: pattern={s['pattern_present']}, extracts={s['reaction_extract_count']}, queryable_mols={s['queryable_molecule_count']}")
    lines.append("\n## Alias status\n")
    for a in summary["alias_status"]:
        lines.append(f"- {a['long_alias']} -> {a['short_name']}: synonym={a['short_synonym_contains_long']}, abbreviation_alias={a['abbreviation_alias_pair_found']}, long_shell_present={a['long_shell_present']}")
    lines.append("\n## Possibly unresolved short families\n")
    for fam in summary["possibly_unresolved_short_families"]:
        lines.append(f"- {fam}")
    md_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"summary json: {json_path}")
    print(f"summary md:   {md_path}")
    print("=" * 72)


if __name__ == "__main__":
    raise SystemExit(main())
