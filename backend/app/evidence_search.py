from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

try:
    from rdkit import Chem
    from rdkit.Chem import AllChem, DataStructs
    from rdkit import RDLogger
except Exception as e:  # pragma: no cover
    Chem = None  # type: ignore
    AllChem = None  # type: ignore
    DataStructs = None  # type: ignore
    RDLogger = None  # type: ignore
    _RDKIT_ERR = str(e)
else:
    _RDKIT_ERR = None
    RDLogger.DisableLog("rdApp.*")

router = APIRouter()

APP_DIR = Path(__file__).resolve().parent
DEFAULT_DB = APP_DIR / "labint_round9_bridge_work.db"
DB_PATH = Path(os.environ.get("LABINT_DB_PATH", str(DEFAULT_DB)))


def _db_connect() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise HTTPException(status_code=500, detail=f"Evidence DB not found: {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_rdkit() -> None:
    if Chem is None:
        raise HTTPException(status_code=500, detail=f"RDKit import failed: {_RDKIT_ERR}")


class StructureEvidenceRequest(BaseModel):
    smiles: Optional[str] = None
    reaction_smiles: Optional[str] = None
    top_k: int = 12
    min_tanimoto: float = 0.25
    include_family_fallback: bool = True


_cache_lock = threading.Lock()
_cache_state: Dict[str, Any] = {
    "db_path": None,
    "db_mtime": None,
    "tier1_rows": [],
    "tier2_rows": [],
}


ROLE_WEIGHTS = {
    "product": 1.20,
    "reactant": 1.00,
    "agent": 0.75,
}


def _norm_role(role: Optional[str]) -> str:
    r = str(role or "unknown").strip().lower()
    if r in {"product", "products"}:
        return "product"
    if r in {"reactant", "reactants", "substrate", "starting_material"}:
        return "reactant"
    if r in {"reagent", "reagents", "catalyst", "solvent", "agent", "agents"}:
        return "agent"
    return "unknown"


def _clean_component(text: str) -> str:
    return (text or "").strip()


def _parse_reaction_smiles(text: str) -> Dict[str, List[str]]:
    if not text or ">" not in text:
        raise HTTPException(status_code=400, detail="reaction_smiles must contain a reaction arrow")

    parts = text.split(">")
    if len(parts) == 3:
        reactants_raw, agents_raw, products_raw = parts
    elif len(parts) == 2:
        reactants_raw, products_raw = parts
        agents_raw = ""
    else:
        raise HTTPException(status_code=400, detail="Unsupported reaction SMILES format")

    def split_side(side: str) -> List[str]:
        return [_clean_component(p) for p in side.split(".") if _clean_component(p)]

    return {
        "reactants": split_side(reactants_raw),
        "agents": split_side(agents_raw),
        "products": split_side(products_raw),
    }


class _QueryMol(Tuple[str, str, Any, Any, str]):
    pass


def _build_query_molecules(smiles: str, role: str = "unknown") -> List[Dict[str, Any]]:
    _ensure_rdkit()
    components = [_clean_component(p) for p in str(smiles or "").split(".") if _clean_component(p)]
    if not components:
        raise HTTPException(status_code=400, detail="Invalid or empty SMILES")

    out: List[Dict[str, Any]] = []
    errors: List[str] = []
    for comp in components:
        mol = Chem.MolFromSmiles(comp)
        if mol is None:
            errors.append(comp)
            continue
        canon = Chem.MolToSmiles(mol, canonical=True)
        fp = AllChem.GetMorganFingerprintAsBitVect(mol, 2, nBits=2048)
        out.append({"role": _norm_role(role), "raw": comp, "smiles": canon, "mol": mol, "fp": fp})
    if not out:
        raise HTTPException(status_code=400, detail=f"Invalid SMILES component(s): {', '.join(errors[:5])}")
    return out



def _load_cache_if_needed() -> None:
    _ensure_rdkit()
    db_mtime = DB_PATH.stat().st_mtime if DB_PATH.exists() else None
    with _cache_lock:
        if _cache_state["db_path"] == str(DB_PATH) and _cache_state["db_mtime"] == db_mtime:
            return

        conn = _db_connect()
        try:
            tier1_rows = []
            for row in conn.execute(
                """
                SELECT id, extract_id, role, smiles, reaction_family_name, source_zip, page_no
                FROM extract_molecules
                WHERE queryable = 1 AND quality_tier = 1 AND smiles IS NOT NULL
                """
            ):
                mol = Chem.MolFromSmiles(row["smiles"])
                if mol is None:
                    continue
                fp = AllChem.GetMorganFingerprintAsBitVect(mol, 2, nBits=2048)
                tier1_rows.append(
                    {
                        "id": row["id"],
                        "extract_id": row["extract_id"],
                        "role": _norm_role(row["role"]),
                        "smiles": row["smiles"],
                        "reaction_family_name": row["reaction_family_name"],
                        "source_zip": row["source_zip"],
                        "page_no": row["page_no"],
                        "mol": mol,
                        "fp": fp,
                    }
                )

            tier2_rows = []
            for row in conn.execute(
                """
                SELECT id, extract_id, role, smiles, reaction_family_name, source_zip, page_no
                FROM extract_molecules
                WHERE queryable = 1 AND quality_tier = 2 AND smiles IS NOT NULL
                """
            ):
                qmol = Chem.MolFromSmarts(row["smiles"])
                if qmol is None:
                    continue
                tier2_rows.append(
                    {
                        "id": row["id"],
                        "extract_id": row["extract_id"],
                        "role": _norm_role(row["role"]),
                        "smarts": row["smiles"],
                        "reaction_family_name": row["reaction_family_name"],
                        "source_zip": row["source_zip"],
                        "page_no": row["page_no"],
                        "qmol": qmol,
                    }
                )
        finally:
            conn.close()

        _cache_state["db_path"] = str(DB_PATH)
        _cache_state["db_mtime"] = db_mtime
        _cache_state["tier1_rows"] = tier1_rows
        _cache_state["tier2_rows"] = tier2_rows



def _fetch_extract_details(extract_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not extract_ids:
        return {}
    conn = _db_connect()
    try:
        placeholders = ",".join("?" for _ in extract_ids)
        rows = conn.execute(
            f"""
            SELECT
                re.id AS extract_id,
                re.reaction_family_name,
                re.extract_kind,
                re.transformation_text,
                re.reactants_text,
                re.products_text,
                re.reagents_text,
                re.conditions_text,
                re.temperature_text,
                re.time_text,
                re.yield_text,
                sc.section_type,
                sc.scheme_role,
                pi.page_no,
                pi.source_zip,
                pi.image_filename
            FROM reaction_extracts re
            JOIN scheme_candidates sc ON re.scheme_candidate_id = sc.id
            JOIN page_images pi ON sc.page_image_id = pi.id
            WHERE re.id IN ({placeholders})
            """,
            extract_ids,
        ).fetchall()
        return {int(r["extract_id"]): dict(r) for r in rows}
    finally:
        conn.close()


@router.get("/search/structure-evidence/stats")
def structure_evidence_stats():
    conn = _db_connect()
    try:
        total = conn.execute("SELECT COUNT(*) FROM extract_molecules").fetchone()[0]
        tier_counts = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT quality_tier, COUNT(*) FROM extract_molecules GROUP BY quality_tier ORDER BY quality_tier"
            ).fetchall()
        }
        families = conn.execute(
            "SELECT COUNT(DISTINCT reaction_family_name) FROM extract_molecules WHERE reaction_family_name IS NOT NULL"
        ).fetchone()[0]
        extracts = conn.execute("SELECT COUNT(*) FROM reaction_extracts").fetchone()[0]
        pages = conn.execute("SELECT COUNT(*) FROM page_images").fetchone()[0]
        family_patterns = conn.execute("SELECT COUNT(*) FROM reaction_family_patterns").fetchone()[0] if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reaction_family_patterns'").fetchone() else 0
        aliases = conn.execute("SELECT COUNT(*) FROM abbreviation_aliases").fetchone()[0] if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='abbreviation_aliases'").fetchone() else 0
        entities = conn.execute("SELECT COUNT(*) FROM extract_entities").fetchone()[0] if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='extract_entities'").fetchone() else 0
        return {
            "db_path": str(DB_PATH),
            "page_images": pages,
            "reaction_extracts": extracts,
            "extract_molecules": total,
            "families": families,
            "family_patterns": family_patterns,
            "abbreviation_aliases": aliases,
            "extract_entities": entities,
            "tier1": tier_counts.get(1, 0),
            "tier2": tier_counts.get(2, 0),
            "tier3": tier_counts.get(3, 0),
        }
    finally:
        conn.close()


@router.get("/search/structure-evidence")
def structure_evidence_get(
    smiles: Optional[str] = Query(None),
    reaction_smiles: Optional[str] = Query(None),
    family: Optional[str] = Query(None),
    top_k: int = Query(12, ge=1, le=50),
    min_tanimoto: float = Query(0.25, ge=0.0, le=1.0),
    include_family_fallback: bool = Query(True),
):
    if reaction_smiles:
        req = StructureEvidenceRequest(
            reaction_smiles=reaction_smiles,
            top_k=top_k,
            min_tanimoto=min_tanimoto,
            include_family_fallback=include_family_fallback,
        )
        return _search_by_reaction(req)
    if family and not smiles:
        return _search_by_family(family=family, top_k=top_k)
    if not smiles:
        raise HTTPException(status_code=400, detail="Provide smiles, reaction_smiles, or family")
    req = StructureEvidenceRequest(
        smiles=smiles,
        top_k=top_k,
        min_tanimoto=min_tanimoto,
        include_family_fallback=include_family_fallback,
    )
    return _search_by_structure(req)


@router.post("/search/structure-evidence")
def structure_evidence_post(req: StructureEvidenceRequest):
    if req.reaction_smiles:
        return _search_by_reaction(req)
    if req.smiles:
        return _search_by_structure(req)
    raise HTTPException(status_code=400, detail="Provide smiles or reaction_smiles")





def _fetch_family_profile(conn: sqlite3.Connection, family: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT family_name, family_name_norm, family_class, transformation_type, mechanism_type,
               reactant_pattern_text, product_pattern_text, key_reagents_clue, common_solvents,
               common_conditions, description_short, evidence_extract_count, overview_count,
               application_count, mechanism_count
        FROM reaction_family_patterns
        WHERE lower(family_name) = lower(?) OR lower(family_name_norm) = lower(?)
        LIMIT 1
        """,
        (family, family),
    ).fetchone()
    return dict(row) if row else None


def _search_by_family(family: str, top_k: int = 12) -> Dict[str, Any]:
    conn = _db_connect()
    try:
        rows = conn.execute(
            """
            SELECT
                re.id AS extract_id,
                re.reaction_family_name,
                re.extract_kind,
                re.transformation_text,
                re.reactants_text,
                re.products_text,
                re.reagents_text,
                re.conditions_text,
                re.temperature_text,
                re.time_text,
                re.yield_text,
                sc.section_type,
                sc.scheme_role,
                pi.page_no,
                pi.source_zip,
                pi.image_filename
            FROM reaction_extracts re
            JOIN scheme_candidates sc ON re.scheme_candidate_id = sc.id
            JOIN page_images pi ON sc.page_image_id = pi.id
            WHERE lower(re.reaction_family_name) LIKE ?
            ORDER BY pi.page_no, re.id
            LIMIT ?
            """,
            (f"%{family.lower()}%", top_k),
        ).fetchall()
        return {
            "query_mode": "family",
            "query_family": family,
            "direct_count": 0,
            "generic_count": 0,
            "family_count": len(rows),
            "family_profile": family_profile,
            "results": [
                {
                    **dict(row),
                    "match_type": "family_text",
                    "role": "family",
                    "match_score": 0.0,
                    "matched_smiles": None,
                    "quality_tier": 3,
                }
                for row in rows
            ],
        }
    finally:
        conn.close()


def _role_bonus(query_role: str, indexed_role: str) -> float:
    q = _norm_role(query_role)
    i = _norm_role(indexed_role)
    if q == "unknown" or i == "unknown":
        return 1.0
    if q == i:
        return 1.15
    if {q, i} == {"agent", "reactant"}:
        return 0.95
    return 0.88



def _append_component_hit(target: Dict[int, Dict[str, Any]], extract_id: int, payload: Dict[str, Any]) -> None:
    slot = target.setdefault(
        extract_id,
        {
            "extract_id": extract_id,
            "match_score": 0.0,
            "quality_tier": payload["quality_tier"],
            "role": payload.get("matched_role") or payload.get("query_role") or "unknown",
            "matched_components": [],
        },
    )
    slot["match_score"] += float(payload.get("match_score") or 0.0)
    slot["quality_tier"] = min(int(slot.get("quality_tier") or 99), int(payload["quality_tier"]))
    comps = slot.setdefault("matched_components", [])
    comps.append(
        {
            "query_role": payload.get("query_role"),
            "query_smiles": payload.get("query_smiles"),
            "matched_role": payload.get("matched_role"),
            "matched_smiles": payload.get("matched_smiles"),
            "match_type": payload.get("match_type"),
            "match_score": round(float(payload.get("match_score") or 0.0), 3),
        }
    )



def _family_fallback_from_families(family_names: List[str], used_extract_ids: List[int], top_k: int) -> List[Dict[str, Any]]:
    family_names = [f for f in family_names if f]
    family_names = list(dict.fromkeys(family_names))[:5]
    if not family_names or top_k <= 0:
        return []
    conn = _db_connect()
    try:
        placeholders = ",".join("?" for _ in family_names)
        exclude_sql = ",".join("?" for _ in used_extract_ids) if used_extract_ids else "0"
        rows = conn.execute(
            f"""
            SELECT
                re.id AS extract_id,
                re.reaction_family_name,
                re.extract_kind,
                re.transformation_text,
                re.reactants_text,
                re.products_text,
                re.reagents_text,
                re.conditions_text,
                re.temperature_text,
                re.time_text,
                re.yield_text,
                sc.section_type,
                sc.scheme_role,
                pi.page_no,
                pi.source_zip,
                pi.image_filename
            FROM reaction_extracts re
            JOIN scheme_candidates sc ON re.scheme_candidate_id = sc.id
            JOIN page_images pi ON sc.page_image_id = pi.id
            WHERE re.reaction_family_name IN ({placeholders})
              AND re.id NOT IN ({exclude_sql})
            ORDER BY pi.page_no, re.id
            LIMIT ?
            """,
            tuple(family_names) + tuple(used_extract_ids) + (top_k,),
        ).fetchall()
        return [
            {
                **dict(row),
                "match_type": "family_evidence",
                "match_score": 0.10,
                "matched_smiles": None,
                "matched_components": [],
                "quality_tier": 3,
                "role": "family",
            }
            for row in rows
        ]
    finally:
        conn.close()



def _finalize_results(hit_map: Dict[int, Dict[str, Any]], req: StructureEvidenceRequest, query_mode: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    results = list(hit_map.values())
    results.sort(key=lambda x: (-float(x.get("match_score") or 0.0), int(x.get("quality_tier") or 99), int(x["extract_id"])))
    results = results[: req.top_k]

    details = _fetch_extract_details([r["extract_id"] for r in results])
    for item in results:
        detail = details.get(item["extract_id"])
        if detail:
            item.update(detail)
        item["match_score"] = round(float(item.get("match_score") or 0.0), 3)

    family_hits: List[Dict[str, Any]] = []
    if req.include_family_fallback:
        family_names = [item.get("reaction_family_name") for item in results if item.get("reaction_family_name")]
        family_hits = _family_fallback_from_families(
            family_names=family_names,
            used_extract_ids=[r["extract_id"] for r in results],
            top_k=max(0, req.top_k - len(results)),
        )

    final = results + family_hits
    final.sort(key=lambda x: (int(x.get("quality_tier") or 99), -float(x.get("match_score") or 0.0), int(x.get("page_no") or 0)))
    final = final[: req.top_k]

    payload = {
        "query_mode": query_mode,
        "direct_count": len([r for r in results if r.get("quality_tier") == 1]),
        "generic_count": len([r for r in results if r.get("quality_tier") == 2]),
        "family_count": len(family_hits),
        "results": final,
    }
    if results:
        conn = _db_connect()
        try:
            families = [r.get("reaction_family_name") for r in final if r.get("reaction_family_name")]
            families = list(dict.fromkeys(families))[:3]
            if families and conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reaction_family_patterns'").fetchone():
                payload["family_profiles"] = [p for p in (_fetch_family_profile(conn, fam) for fam in families) if p]
        finally:
            conn.close()
    if extra:
        payload.update(extra)
    return payload


def _search_by_structure(req: StructureEvidenceRequest) -> Dict[str, Any]:
    _load_cache_if_needed()
    assert req.smiles
    query_molecules = _build_query_molecules(req.smiles, role="unknown")
    if len(query_molecules) > 1:
        # Disconnected input behaves like a component-set query even without an explicit arrow.
        return _search_by_component_set(req=req, query_molecules=query_molecules, query_mode="mixture", extra={"query_smiles": req.smiles})

    q = query_molecules[0]
    qmol = q["mol"]
    qfp = q["fp"]

    hit_map: Dict[int, Dict[str, Any]] = {}

    for row in _cache_state["tier1_rows"]:
        score = float(DataStructs.TanimotoSimilarity(qfp, row["fp"]))
        if score < req.min_tanimoto:
            continue
        weighted = score * _role_bonus("unknown", row["role"])
        _append_component_hit(
            hit_map,
            row["extract_id"],
            {
                "quality_tier": 1,
                "match_type": "tier1_similarity",
                "match_score": weighted,
                "query_role": "unknown",
                "query_smiles": q["smiles"],
                "matched_role": row["role"],
                "matched_smiles": row["smiles"],
            },
        )

    for row in _cache_state["tier2_rows"]:
        try:
            if qmol.HasSubstructMatch(row["qmol"]):
                weighted = 0.45 * _role_bonus("unknown", row["role"])
                _append_component_hit(
                    hit_map,
                    row["extract_id"],
                    {
                        "quality_tier": 2,
                        "match_type": "tier2_generic",
                        "match_score": weighted,
                        "query_role": "unknown",
                        "query_smiles": q["smiles"],
                        "matched_role": row["role"],
                        "matched_smiles": row["smarts"],
                    },
                )
        except Exception:
            continue

    return _finalize_results(hit_map, req=req, query_mode="structure", extra={"query_smiles": q["smiles"]})



def _search_by_component_set(req: StructureEvidenceRequest, query_molecules: List[Dict[str, Any]], query_mode: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    _load_cache_if_needed()
    hit_map: Dict[int, Dict[str, Any]] = {}

    for q in query_molecules:
        role_weight = ROLE_WEIGHTS.get(q["role"], 1.0)
        for row in _cache_state["tier1_rows"]:
            score = float(DataStructs.TanimotoSimilarity(q["fp"], row["fp"]))
            if score < req.min_tanimoto:
                continue
            weighted = score * role_weight * _role_bonus(q["role"], row["role"])
            _append_component_hit(
                hit_map,
                row["extract_id"],
                {
                    "quality_tier": 1,
                    "match_type": "tier1_similarity",
                    "match_score": weighted,
                    "query_role": q["role"],
                    "query_smiles": q["smiles"],
                    "matched_role": row["role"],
                    "matched_smiles": row["smiles"],
                },
            )

        for row in _cache_state["tier2_rows"]:
            try:
                if q["mol"].HasSubstructMatch(row["qmol"]):
                    weighted = 0.45 * role_weight * _role_bonus(q["role"], row["role"])
                    _append_component_hit(
                        hit_map,
                        row["extract_id"],
                        {
                            "quality_tier": 2,
                            "match_type": "tier2_generic",
                            "match_score": weighted,
                            "query_role": q["role"],
                            "query_smiles": q["smiles"],
                            "matched_role": row["role"],
                            "matched_smiles": row["smarts"],
                        },
                    )
            except Exception:
                continue

    extra_payload = extra or {}
    extra_payload.setdefault(
        "query_components",
        [{"role": q["role"], "smiles": q["smiles"]} for q in query_molecules],
    )
    return _finalize_results(hit_map, req=req, query_mode=query_mode, extra=extra_payload)



def _search_by_reaction(req: StructureEvidenceRequest) -> Dict[str, Any]:
    _load_cache_if_needed()
    assert req.reaction_smiles
    parsed = _parse_reaction_smiles(req.reaction_smiles)
    query_molecules: List[Dict[str, Any]] = []
    for role_key, role_name in (("reactants", "reactant"), ("agents", "agent"), ("products", "product")):
        for item in parsed[role_key]:
            query_molecules.extend(_build_query_molecules(item, role=role_name))
    if not query_molecules:
        raise HTTPException(status_code=400, detail="No valid reaction components found")

    return _search_by_component_set(
        req=req,
        query_molecules=query_molecules,
        query_mode="reaction",
        extra={
            "query_reaction_smiles": req.reaction_smiles,
            "reaction_components": parsed,
        },
    )
