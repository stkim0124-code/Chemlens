"""Phase 4 Gate B eval harness upgrade.

Delivers the 5 Gate-B-blocking items from task #121:

  (1) Unique-edge metric — gained/lost/net edges (cases uniquely moving)
  (3) Broad + coverage case-level diff (alongside admission)
  (5) source_kind split (queryable=1 vs =0) in every diff output
  (6) Regression trace matrix — heuristic per-guard attribution for each case move
  (9) UTF-8-sig BOM handling in all CSV readers

Not a re-runner. Consumes existing snapshot JSONs / CSVs produced by the three
benchmark runners (admission, broad, coverage) and emits a richer diff.

Snapshot layout expected (per phase):
  <snap>/family_admission_results.json
  <snap>/named_reaction_benchmark_broad_results.json
  <snap>/corpus_coverage_results.json

The CSV siblings (family_admission_results.csv, corpus_coverage_results.csv,
family_admission_confusion.csv) are NOT required for diffing — JSON is the
source of truth. CSVs, when read, use encoding="utf-8-sig" (item 9).

Usage
-----
    python scripts/gateB_eval_harness.py diff \
        --before /path/to/baseline_snapshot \
        --after  /path/to/gateB_snapshot \
        --out    /path/to/diff_output_dir \
        [--defects-registry benchmark/defects/benchmark_defects_registry.json] \
        [--guard-registry    scripts/gateB_guard_registry.json]

Outputs (in --out):
  edges_summary.json                 — item (1) + (5): unique-edge + source_kind split
  admission_case_diff.csv            — item (3): admission case-level diff
  broad_case_diff.csv                — item (3): broad case-level diff
  coverage_case_diff.csv             — item (3): coverage case-level diff
  regression_trace_matrix.csv        — item (6): per-case guard attribution (heuristic)
  diff_report.md                     — human summary of the above
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Layer configuration.  Keyed by logical layer name. Each layer declares the
# expected JSON filename, the pass-flag column in row records, and what counts
# as "moved gained" vs "moved lost".
# ---------------------------------------------------------------------------
LAYERS: Dict[str, Dict[str, Any]] = {
    "admission": {
        "json_name": "family_admission_results.json",
        "pass_flag": "top1_correct",
        "denominator_label": "total_cases",
    },
    "broad": {
        # Matches the broad runner's actual output filename. The older
        # `named_reaction_benchmark_broad_results.json` spelling from the
        # task-121 sketch never shipped — real snapshots write `broad_results.json`.
        "json_name": "broad_results.json",
        "pass_flag": "top1_correct",
        "denominator_label": "total_cases",
    },
    "coverage": {
        "json_name": "corpus_coverage_results.json",
        # recall@5 is the Gate B headline; "hit_at_5" is the row-level flag.
        "pass_flag": "hit_at_5",
        "denominator_label": "total_cases",
    },
}


# ---------------------------------------------------------------------------
# I/O helpers — always encoding="utf-8-sig" for CSV readers (item 9).
# ---------------------------------------------------------------------------
def read_json(path: Path) -> Dict[str, Any]:
    """JSON read. UTF-8 is standard; no BOM handling needed here."""
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv_utf8sig(path: Path) -> List[Dict[str, str]]:
    """Item 9: ALL CSV reads in this harness go through this wrapper.

    The family_admission_confusion.csv writer uses encoding='utf-8-sig' which
    prepends a BOM. A plain csv.DictReader then puts the BOM on the first
    column name (\ufeffexpected_canonical), breaking downstream .get() calls.
    Using encoding="utf-8-sig" on read strips the BOM transparently.
    """
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def load_snapshot(snap_dir: Path, layer: str) -> Dict[str, Any]:
    """Load a single layer's JSON from a snapshot directory.

    Two layouts are accepted (real phase3g/phase4 runs use the second):
      (a) flat:     <snap>/family_admission_results.json
      (b) per-layer: <snap>/admission/family_admission_results.json
                     <snap>/broad/broad_results.json
                     <snap>/coverage/corpus_coverage_results.json

    Missing layers (e.g. broad JSON not produced by an older phase) return a
    stub dict with empty rows so the diff tool degrades gracefully.
    """
    json_name = LAYERS[layer]["json_name"]
    candidates = [snap_dir / json_name, snap_dir / layer / json_name]
    path = next((p for p in candidates if p.exists()), candidates[0])
    if not path.exists():
        return {
            "_missing": True,
            "summary": {},
            "rows": [],
            "_path": str(candidates[0]),
            "_searched": [str(p) for p in candidates],
        }
    data = read_json(path)
    data["_missing"] = False
    data["_path"] = str(path)
    return data


# ---------------------------------------------------------------------------
# Unique-edge metric (item 1) + source_kind split (item 5).
# ---------------------------------------------------------------------------
def _row_index(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {r["case_id"]: r for r in rows if r.get("case_id")}


def _source_kind_of(row: Dict[str, Any]) -> str:
    """Best-effort source_kind label. Returns 'queryable_1', 'queryable_0',
    or 'unknown' when the row lacks the field."""
    sk = row.get("source_kind")
    if sk is None or sk == "":
        return "unknown"
    s = str(sk).lower()
    if s in {"1", "true", "queryable", "queryable_1"}:
        return "queryable_1"
    if s in {"0", "false", "not_queryable", "queryable_0", "step4_ocsr"}:
        return "queryable_0"
    return str(sk)


def compute_edges_for_layer(
    before_rows: List[Dict[str, Any]],
    after_rows: List[Dict[str, Any]],
    pass_flag: str,
    defect_case_ids: Optional[set] = None,
) -> Dict[str, Any]:
    """Compute unique-edge metric for a single benchmark layer.

    Definitions (item 1):
      edges_gained = |{c : after.pass[c] and not before.pass[c]}|
      edges_lost   = |{c : before.pass[c] and not after.pass[c]}|
      net_edge     = gained − lost

    Source_kind split (item 5): edges_gained and edges_lost both broken down
    by the source_kind label from the AFTER row (falls back to before row).
    """
    defect_case_ids = defect_case_ids or set()
    before_idx = _row_index(before_rows)
    after_idx = _row_index(after_rows)
    all_case_ids = set(before_idx) | set(after_idx)

    gained: List[str] = []
    lost: List[str] = []
    stayed_pass: List[str] = []
    stayed_fail: List[str] = []
    gained_by_kind: Counter = Counter()
    lost_by_kind: Counter = Counter()

    for cid in sorted(all_case_ids):
        if cid in defect_case_ids:
            continue
        before_pass = bool((before_idx.get(cid) or {}).get(pass_flag, False))
        after_pass = bool((after_idx.get(cid) or {}).get(pass_flag, False))
        ref_row = after_idx.get(cid) or before_idx.get(cid) or {}
        kind = _source_kind_of(ref_row)
        if after_pass and not before_pass:
            gained.append(cid)
            gained_by_kind[kind] += 1
        elif before_pass and not after_pass:
            lost.append(cid)
            lost_by_kind[kind] += 1
        elif after_pass and before_pass:
            stayed_pass.append(cid)
        else:
            stayed_fail.append(cid)

    return {
        "edges_gained": len(gained),
        "edges_lost": len(lost),
        "net_edge_delta": len(gained) - len(lost),
        "stayed_pass": len(stayed_pass),
        "stayed_fail": len(stayed_fail),
        "total_cases_compared": len(all_case_ids) - len(defect_case_ids & all_case_ids),
        "defects_excluded": len(defect_case_ids & all_case_ids),
        "edges_gained_by_source_kind": dict(gained_by_kind),
        "edges_lost_by_source_kind": dict(lost_by_kind),
        "gained_case_ids": gained,
        "lost_case_ids": lost,
    }


# ---------------------------------------------------------------------------
# Case-level diff per layer (item 3).
# ---------------------------------------------------------------------------
def _row_signature(row: Optional[Dict[str, Any]], pass_flag: str) -> Dict[str, Any]:
    if not row:
        return {"pass": None, "top1": None, "top3": None, "source_kind": "unknown"}
    return {
        "pass": bool(row.get(pass_flag, False)),
        "top1": row.get("top1_family"),
        "top3": row.get("top3_families"),
        "source_kind": _source_kind_of(row),
    }


def build_case_diff_rows(
    before_rows: List[Dict[str, Any]],
    after_rows: List[Dict[str, Any]],
    pass_flag: str,
    defect_case_ids: Optional[set] = None,
) -> List[Dict[str, Any]]:
    """Emit one row per case_id that either moved OR whose top1 changed (even
    if the pass/fail flag stayed the same — top1 changes are signal too)."""
    defect_case_ids = defect_case_ids or set()
    before_idx = _row_index(before_rows)
    after_idx = _row_index(after_rows)
    all_ids = sorted(set(before_idx) | set(after_idx))
    out = []
    for cid in all_ids:
        if cid in defect_case_ids:
            continue
        b = before_idx.get(cid)
        a = after_idx.get(cid)
        bs = _row_signature(b, pass_flag)
        as_ = _row_signature(a, pass_flag)
        if bs == as_:
            continue
        direction = "no_change"
        if as_["pass"] and not bs["pass"]:
            direction = "gained"
        elif bs["pass"] and not as_["pass"]:
            direction = "lost"
        elif bs["top1"] != as_["top1"] and bs["pass"] == as_["pass"]:
            direction = "top1_shifted_no_flag_change"
        expected = (a or b or {}).get("expected_family", "")
        out.append({
            "case_id": cid,
            "direction": direction,
            "expected_family": expected,
            "before_pass": bs["pass"],
            "after_pass": as_["pass"],
            "before_top1": bs["top1"] or "",
            "after_top1": as_["top1"] or "",
            "before_top3": bs["top3"] or "",
            "after_top3": as_["top3"] or "",
            "source_kind": as_["source_kind"],
        })
    return out


# ---------------------------------------------------------------------------
# Regression trace matrix — heuristic attribution (item 6).
# ---------------------------------------------------------------------------
def attribute_to_guards(
    diff_rows: List[Dict[str, Any]],
    guards: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """For each moved case, match the expected_family / before_top1 / after_top1
    against each guard's family_keywords. Return a per-case attribution.

    Confidence levels:
      high      — exactly one guard matches, and its expected_move_direction
                  aligns with the observed direction
      medium    — exactly one guard matches but direction doesn't align
      ambiguous — multiple guards match
      none      — no guard matches (move is likely due to SMARTS feature-count
                  propagation or an untracked interaction)
    """
    def match_guard(g: Dict[str, Any], tokens: List[str]) -> bool:
        for kw in g.get("family_keywords", []):
            k = kw.lower()
            for t in tokens:
                if k and t and k in t:
                    return True
        return False

    out = []
    for row in diff_rows:
        tokens = [
            (row.get("expected_family") or "").lower(),
            (row.get("before_top1") or "").lower(),
            (row.get("after_top1") or "").lower(),
        ]
        matched = [g for g in guards if match_guard(g, tokens)]
        if len(matched) == 0:
            attribution = {
                "attributed_guards": "",
                "confidence": "none",
                "reason": "no family keyword match across expected/top1_before/top1_after",
            }
        elif len(matched) == 1:
            g = matched[0]
            direction = row.get("direction")
            expected_dir = g.get("expected_move_direction", "")
            aligns = False
            if direction == "gained" and expected_dir.startswith("gain_"):
                aligns = True
            elif direction == "lost" and expected_dir.startswith("shed_"):
                aligns = True
            elif direction == "top1_shifted_no_flag_change":
                aligns = True
            attribution = {
                "attributed_guards": g["id"],
                "confidence": "high" if aligns else "medium",
                "reason": f"single guard match ({g['id']}); expected_dir={expected_dir}; observed={direction}",
            }
        else:
            attribution = {
                "attributed_guards": ";".join(g["id"] for g in matched),
                "confidence": "ambiguous",
                "reason": f"{len(matched)} guards matched — manual triage needed",
            }
        out.append({**row, **attribution})
    return out


# ---------------------------------------------------------------------------
# Defects registry (item 9 tie-in: a --defects-registry flag).
# ---------------------------------------------------------------------------
def load_defect_case_ids(path: Optional[Path]) -> Tuple[set, Dict[str, Any]]:
    if path is None:
        return set(), {"enabled": False, "defect_count": 0, "case_ids": []}
    if not path.exists():
        return set(), {"enabled": False, "defect_count": 0, "missing_path": str(path)}
    reg = read_json(path)
    cids = set()
    for entry in reg.get("defects", []):
        cid = entry.get("case_id") or entry.get("canonical_case_id")
        if cid:
            cids.add(cid)
    return cids, {
        "enabled": True,
        "registry_path": str(path),
        "defect_count": len(cids),
        "case_ids": sorted(cids),
    }


# ---------------------------------------------------------------------------
# Report writers.
# ---------------------------------------------------------------------------
def write_case_diff_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "case_id", "direction", "expected_family",
        "before_pass", "after_pass",
        "before_top1", "after_top1",
        "before_top3", "after_top3",
        "source_kind",
        "attributed_guards", "confidence", "reason",
    ]
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _fmt_pct(x: float) -> str:
    return f"{x*100:.2f}%"


def write_markdown_report(path: Path, edges_summary: Dict[str, Any]) -> None:
    """Human-readable rollup of the diff run."""
    lines = ["# Gate B eval harness diff report", ""]
    meta = edges_summary.get("_meta", {})
    lines.append(f"- **before:** `{meta.get('before_snap_dir', '?')}`")
    lines.append(f"- **after:** `{meta.get('after_snap_dir', '?')}`")
    defects = edges_summary.get("_defects", {})
    if defects.get("enabled"):
        lines.append(f"- **defects excluded:** {defects['defect_count']} "
                     f"({', '.join(defects.get('case_ids', [])) or 'none'})")
    lines.append("")
    lines.append("## Per-layer unique-edge metric")
    lines.append("")
    lines.append("| layer | total | gained | lost | net | stayed_pass | stayed_fail |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for layer in ["admission", "broad", "coverage"]:
        m = edges_summary.get(layer, {})
        if m.get("_missing"):
            lines.append(f"| {layer} | (missing) | | | | | |")
            continue
        lines.append(
            f"| {layer} | {m.get('total_cases_compared', 0)} "
            f"| {m.get('edges_gained', 0)} "
            f"| {m.get('edges_lost', 0)} "
            f"| {m.get('net_edge_delta', 0):+d} "
            f"| {m.get('stayed_pass', 0)} "
            f"| {m.get('stayed_fail', 0)} |"
        )
    lines.append("")
    lines.append("## source_kind split (gained | lost)")
    lines.append("")
    lines.append("| layer | queryable_1 (+/−) | queryable_0 (+/−) | unknown (+/−) |")
    lines.append("|---|:---:|:---:|:---:|")
    for layer in ["admission", "broad", "coverage"]:
        m = edges_summary.get(layer, {})
        if m.get("_missing"):
            lines.append(f"| {layer} | (missing) | | |")
            continue
        g = m.get("edges_gained_by_source_kind", {})
        l = m.get("edges_lost_by_source_kind", {})
        def cell(k):
            return f"{g.get(k, 0)} / {l.get(k, 0)}"
        lines.append(f"| {layer} | {cell('queryable_1')} | {cell('queryable_0')} | {cell('unknown')} |")
    lines.append("")
    gate = edges_summary.get("_gate_verdict", {})
    if gate:
        lines.append("## Gate verdict")
        lines.append("")
        lines.append(f"- **pass?** {gate.get('pass')} — {gate.get('reason', '')}")
        lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Main — `diff` subcommand.
# ---------------------------------------------------------------------------
def cmd_diff(args) -> int:
    before_dir = Path(args.before).resolve()
    after_dir = Path(args.after).resolve()
    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    defect_cids, defects_meta = load_defect_case_ids(
        Path(args.defects_registry).resolve() if args.defects_registry else None
    )

    guard_registry = None
    guards: List[Dict[str, Any]] = []
    if args.guard_registry:
        guard_path = Path(args.guard_registry).resolve()
        guard_registry = read_json(guard_path)
        guards = guard_registry.get("guards", [])

    edges_summary: Dict[str, Any] = {
        "_meta": {
            "before_snap_dir": str(before_dir),
            "after_snap_dir": str(after_dir),
            "out_dir": str(out_dir),
        },
        "_defects": defects_meta,
        "_guard_registry": str(args.guard_registry) if args.guard_registry else None,
    }

    all_attributed: List[Dict[str, Any]] = []

    for layer, cfg in LAYERS.items():
        before_snap = load_snapshot(before_dir, layer)
        after_snap = load_snapshot(after_dir, layer)
        if before_snap["_missing"] or after_snap["_missing"]:
            edges_summary[layer] = {
                "_missing": True,
                "before_path": before_snap.get("_path"),
                "after_path": after_snap.get("_path"),
                "before_missing": before_snap["_missing"],
                "after_missing": after_snap["_missing"],
            }
            continue

        edges = compute_edges_for_layer(
            before_snap.get("rows") or [],
            after_snap.get("rows") or [],
            pass_flag=cfg["pass_flag"],
            defect_case_ids=defect_cids,
        )
        edges_summary[layer] = edges

        case_diff = build_case_diff_rows(
            before_snap.get("rows") or [],
            after_snap.get("rows") or [],
            pass_flag=cfg["pass_flag"],
            defect_case_ids=defect_cids,
        )
        if guards:
            case_diff = attribute_to_guards(case_diff, guards)
            all_attributed.extend(
                {**r, "layer": layer} for r in case_diff
                if r.get("direction") in {"gained", "lost", "top1_shifted_no_flag_change"}
            )

        write_case_diff_csv(out_dir / f"{layer}_case_diff.csv", case_diff)

    # Top-level gate verdict (non-binding; a human still reads the MD).
    adm = edges_summary.get("admission") or {}
    cov = edges_summary.get("coverage") or {}
    verdict_pass = None
    reason = ""
    if adm and not adm.get("_missing") and cov and not cov.get("_missing"):
        # Soft heuristic Gate B threshold; the hard thresholds live in
        # apply_summary.json.expected_impact_notes and the user's judgment.
        adm_net = adm.get("net_edge_delta", 0)
        cov_net = cov.get("net_edge_delta", 0)
        verdict_pass = adm_net > 0 and cov_net >= 0
        reason = (
            f"admission net={adm_net:+d}, coverage net={cov_net:+d}; "
            f"heuristic rule: admission must gain and coverage must not regress."
        )
    edges_summary["_gate_verdict"] = {"pass": verdict_pass, "reason": reason}

    (out_dir / "edges_summary.json").write_text(
        json.dumps(edges_summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Unified regression trace matrix — union across all three layers.
    if guards and all_attributed:
        trace_path = out_dir / "regression_trace_matrix.csv"
        fieldnames = [
            "layer", "case_id", "direction", "expected_family",
            "before_top1", "after_top1",
            "source_kind",
            "attributed_guards", "confidence", "reason",
        ]
        with trace_path.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for r in all_attributed:
                w.writerow(r)
        edges_summary["_regression_trace_path"] = str(trace_path)

    write_markdown_report(out_dir / "diff_report.md", edges_summary)

    # Stdout recap — machine-readable one-liner for CI/logs.
    print(json.dumps({
        "status": "ok",
        "layers_compared": [k for k in ("admission", "broad", "coverage")
                            if not edges_summary.get(k, {}).get("_missing")],
        "layers_missing": [k for k in ("admission", "broad", "coverage")
                           if edges_summary.get(k, {}).get("_missing")],
        "admission_net_edge": (edges_summary.get("admission") or {}).get("net_edge_delta"),
        "coverage_net_edge": (edges_summary.get("coverage") or {}).get("net_edge_delta"),
        "out_dir": str(out_dir),
    }, ensure_ascii=False))
    return 0


def cmd_selftest(args) -> int:
    """Quick self-test: synthesize two tiny before/after payloads in memory
    and assert the unique-edge / source_kind / attribution logic is correct.
    Runs offline — no DB, no benchmark files needed. """
    import tempfile, os

    def mk_row(cid, exp, top1, passed, sk):
        return {
            "case_id": cid,
            "expected_family": exp,
            "top1_family": top1,
            "top3_families": top1 or "",
            "top1_correct": passed,
            "hit_at_5": passed,
            "source_kind": sk,
        }

    before_rows = [
        mk_row("c1", "Cope Elimination",     "Hofmann Elimination", False, "1"),
        mk_row("c2", "Sandmeyer Reaction",   "Ullmann Reaction",    False, "0"),
        mk_row("c3", "Chugaev Elimination",  "Chugaev Elimination", True,  "1"),
        mk_row("c4", "Wolff-Kishner",        "Wolff-Kishner",       True,  "1"),
        mk_row("c5", "Stille Coupling",      "Stille Coupling",     True,  "0"),
    ]
    after_rows = [
        mk_row("c1", "Cope Elimination",     "Cope Elimination",    True,  "1"),  # gained
        mk_row("c2", "Sandmeyer Reaction",   "Sandmeyer Reaction",  True,  "0"),  # gained, q0
        mk_row("c3", "Chugaev Elimination",  "RCM",                 False, "1"),  # lost
        mk_row("c4", "Wolff-Kishner",        "Wolff-Kishner",       True,  "1"),  # stayed
        mk_row("c5", "Stille Coupling",      "Stille Coupling",     True,  "0"),  # stayed
    ]

    edges = compute_edges_for_layer(before_rows, after_rows, "top1_correct")
    assert edges["edges_gained"] == 2, f"expected 2 gained, got {edges['edges_gained']}"
    assert edges["edges_lost"] == 1, f"expected 1 lost, got {edges['edges_lost']}"
    assert edges["net_edge_delta"] == 1
    assert edges["edges_gained_by_source_kind"].get("queryable_1") == 1
    assert edges["edges_gained_by_source_kind"].get("queryable_0") == 1
    assert edges["edges_lost_by_source_kind"].get("queryable_1") == 1

    diff = build_case_diff_rows(before_rows, after_rows, "top1_correct")
    assert len(diff) == 3

    # Guard attribution test
    guards = [
        {"id": "G01", "family_keywords": ["cope elimination"], "expected_move_direction": "gain_cope_from_hofmann"},
        {"id": "G09", "family_keywords": ["sandmeyer"],        "expected_move_direction": "gain_sandmeyer_where_diazonium"},
        {"id": "G08", "family_keywords": ["chugaev"],          "expected_move_direction": "shed_chugaev_where_no_xanthate"},
    ]
    attributed = attribute_to_guards(diff, guards)
    by_cid = {r["case_id"]: r for r in attributed}
    assert by_cid["c1"]["attributed_guards"] == "G01"
    assert by_cid["c1"]["confidence"] == "high"
    assert by_cid["c2"]["attributed_guards"] == "G09"
    assert by_cid["c2"]["confidence"] == "high"
    assert by_cid["c3"]["attributed_guards"] == "G08"
    assert by_cid["c3"]["confidence"] == "high"

    # UTF-8-sig CSV round-trip
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "bom_test.csv"
        p.write_bytes("\ufeffcase_id,value\nc1,hello\n".encode("utf-8"))
        rows = read_csv_utf8sig(p)
        assert list(rows[0].keys()) == ["case_id", "value"], rows[0].keys()
        assert rows[0]["case_id"] == "c1"

    # Defects registry
    with tempfile.TemporaryDirectory() as td:
        regp = Path(td) / "defects.json"
        regp.write_text(json.dumps({
            "defects": [{"case_id": "c5", "reason": "test"}]
        }), encoding="utf-8")
        cids, meta = load_defect_case_ids(regp)
        assert cids == {"c5"}
        assert meta["defect_count"] == 1

    print("[selftest] ALL OK — 5 items covered (edges, source_kind split, case diff, guard attribution, BOM + defects)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Gate B eval harness upgrade")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("diff", help="Diff two benchmark snapshot dirs")
    sp.add_argument("--before", required=True, help="Snapshot dir with the 3 layer JSONs (baseline)")
    sp.add_argument("--after", required=True, help="Snapshot dir with the 3 layer JSONs (post-patch)")
    sp.add_argument("--out", required=True, help="Output directory for diff artifacts")
    sp.add_argument("--defects-registry", default=None, help="Optional defects registry JSON to exclude cases from denominators")
    sp.add_argument("--guard-registry", default=None, help="Optional guard registry JSON for regression-trace attribution (item 6)")
    sp.set_defaults(fn=cmd_diff)

    sp = sub.add_parser("selftest", help="Run offline self-test covering all 5 items")
    sp.set_defaults(fn=cmd_selftest)

    ns = ap.parse_args()
    return ns.fn(ns)


if __name__ == "__main__":
    raise SystemExit(main())
