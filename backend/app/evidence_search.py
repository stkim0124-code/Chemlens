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
DEFAULT_DB = APP_DIR / "labint.db"
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


FAMILY_NAME_KO = {
    "Buchner Reaction": "부흐너 고리 확장 반응",
    "Burgess Dehydration Reaction": "버지스 탈수 반응",
    "Amadori Rearrangement": "아마도리 재배열",
    "Amadori Reaction / Rearrangement": "아마도리 반응/재배열",
    "Beckmann Rearrangement": "베크만 재배열",
    "Baeyer-Villiger Oxidation": "바이어-빌리거 산화",
    "Baeyer-Villiger Oxidation/Rearrangement": "바이어-빌리거 산화/재배열",
    "Barton Radical Decarboxylation Reaction": "바튼 라디칼 탈카복실화 반응",
    "Barton-McCombie Radical Deoxygenation Reaction": "바튼-맥컴비 라디칼 탈산소화 반응",
    "Barton Nitrite Ester Reaction": "바튼 나이트라이트 에스터 반응",
    "Baylis-Hillman Reaction": "베일리스-힐만 반응",
    "Buchwald-Hartwig Cross-Coupling": "부흐발트-하트비히 교차 커플링",
    "Claisen Rearrangement": "클라이젠 재배열",
    "Claisen-Ireland Rearrangement": "클라이젠-아일랜드 재배열",
    "Claisen Condensation / Claisen Reaction": "클라이젠 축합 반응",
    "Chugaev Elimination Reaction": "추가예프 제거 반응",
    "Clemmensen Reduction": "클레멘젠 환원",
    "Brook Rearrangement": "브룩 재배열",
    "Brown Hydroboration Reaction": "브라운 하이드로보레이션 반응",
    "Bischler-Napieralski Isoquinoline Synthesis": "비슐러-나피에랄스키 아이소퀴놀린 합성",
    "Biginelli Reaction": "비기넬리 반응",
    "Buchner Method of Ring Expansion": "부흐너 고리 확장법",
    "Chichibabin Amination Reaction": "치치바빈 아미노화 반응",
    "Ciamician-Dennstedt Rearrangement": "치아미치안-덴슈테트 재배열",
    "Castro-Stephens Coupling": "카스트로-스티븐스 커플링",
}

REACTION_CLASS_KO = {
    'rearrangement':'재배열', 'oxidation':'산화', 'reduction':'환원', 'coupling':'커플링',
    'cyclization':'고리화', 'ring_expansion':'고리 확장', 'elimination':'제거반응',
    'substitution':'치환반응', 'amination':'아미노화', 'condensation':'축합', 'other':'기타'
}

def _truncate_text(text: Optional[str], max_len: int = 120) -> str:
    s = ' '.join(str(text or '').replace('\n', ' ').split())
    if not s:
        return ''
    return s if len(s) <= max_len else s[: max_len - 1].rstrip() + '…'

def _display_name_ko(name: Optional[str]) -> str:
    return FAMILY_NAME_KO.get(str(name or '').strip(), '')

def _reaction_class_ko(item: Dict[str, Any], family_profile: Optional[Dict[str, Any]]) -> str:
    cand = None
    if family_profile:
        cand = family_profile.get('transformation_type') or family_profile.get('family_class') or family_profile.get('mechanism_type')
    cand = str(cand or item.get('extract_kind') or '').strip().lower()
    if 'rearr' in cand:
        return '재배열'
    if 'oxid' in cand:
        return '산화'
    if 'reduct' in cand:
        return '환원'
    if 'coupl' in cand:
        return '커플링'
    if 'cycl' in cand or 'annulation' in cand:
        return '고리화'
    if 'elimin' in cand:
        return '제거반응'
    if 'aminat' in cand:
        return '아미노화'
    if 'condens' in cand:
        return '축합'
    return REACTION_CLASS_KO.get(cand, '기타')

def _confidence_label(score: float) -> str:
    if score >= 1.2:
        return '높음'
    if score >= 0.45:
        return '중간'
    return '참고용'

def _summarize_change(item: Dict[str, Any], family_profile: Optional[Dict[str, Any]]) -> str:
    txt = _truncate_text(item.get('transformation_text'))
    if txt:
        low = txt.lower()
        reps = [('ketone','케톤'),('aldehyde','알데하이드'),('alcohol','알코올'),('ester','에스터'),('lactone','락톤'),('amide','아마이드'),('amine','아민'),('aryl halide','아릴 할라이드'),('oxime','옥심'),('acid','산'),('alkene','알켄')]
        for a,b in reps:
            low = low.replace(a,b)
        return _truncate_text(low[:1].upper()+low[1:], 110)
    if family_profile:
        rp = _truncate_text(family_profile.get('reactant_pattern_text'), 40)
        pp = _truncate_text(family_profile.get('product_pattern_text'), 40)
        if rp or pp:
            return f'{rp or "출발물질"} → {pp or "생성물"}'
        ds = _truncate_text(family_profile.get('description_short'), 110)
        if ds:
            return ds
    return ''

def _summarize_reagents(item: Dict[str, Any], family_profile: Optional[Dict[str, Any]]) -> str:
    text = item.get('reagents_text') or (family_profile or {}).get('key_reagents_clue')
    return _truncate_text(text, 90)

def _summarize_conditions(item: Dict[str, Any], family_profile: Optional[Dict[str, Any]]) -> str:
    parts=[]
    cond = _truncate_text(item.get('conditions_text') or (family_profile or {}).get('common_conditions'), 50)
    temp = _truncate_text(item.get('temperature_text'), 24)
    time = _truncate_text(item.get('time_text'), 24)
    if cond: parts.append(cond)
    if temp: parts.append(temp)
    if time: parts.append(time)
    if not parts and family_profile:
        sol = _truncate_text(family_profile.get('common_solvents'), 30)
        if sol: parts.append(sol)
    return ' · '.join(parts[:3])

def _summarize_yield(item: Dict[str, Any]) -> str:
    y = _truncate_text(item.get('yield_text'), 24)
    return y

def _substrate_scope_hint(item: Dict[str, Any], family_profile: Optional[Dict[str, Any]]) -> str:
    text = (family_profile or {}).get('reactant_pattern_text') or item.get('reactants_text')
    return _truncate_text(text, 80)

def _product_type_hint(item: Dict[str, Any], family_profile: Optional[Dict[str, Any]]) -> str:
    text = (family_profile or {}).get('product_pattern_text') or item.get('products_text')
    return _truncate_text(text, 80)

def _naturalize_item(item: Dict[str, Any], family_profile: Optional[Dict[str, Any]]) -> None:
    score = float(item.get('match_score') or 0.0)
    item['family_profile'] = family_profile
    item['display_name_en'] = item.get('reaction_family_name') or '(family 없음)'
    item['display_name_ko'] = _display_name_ko(item.get('reaction_family_name'))
    item['reaction_class_ko'] = _reaction_class_ko(item, family_profile)
    item['confidence_label'] = _confidence_label(score)
    item['key_change_summary'] = _summarize_change(item, family_profile)
    item['key_reagents_summary'] = _summarize_reagents(item, family_profile)
    item['key_conditions_summary'] = _summarize_conditions(item, family_profile)
    item['yield_summary'] = _summarize_yield(item)
    item['substrate_scope_hint'] = _substrate_scope_hint(item, family_profile)
    item['product_type_hint'] = _product_type_hint(item, family_profile)
    item['source_page'] = f"p.{item.get('page_no') or '?'}"

# Patch-2: extract_kind quality multiplier
# canonical_overview / overview → highest representativeness
# mechanism → reliable but reaction-step-specific
# application_example / synthetic_application → noisier (specific substrate)
EXTRACT_KIND_WEIGHTS: Dict[str, float] = {
    "canonical_overview": 1.20,
    "overview": 1.15,
    "mechanism": 1.10,
    "mechanism_step": 1.05,
    "application_example": 0.85,
    "synthetic_application": 0.85,
}

# Patch-3: small fragment penalty
# Molecules with heavy-atom count ≤ this threshold get a score penalty.
SMALL_FRAGMENT_HA_THRESHOLD = 6       # ≤6 heavy atoms → common solvent/reagent range
SMALL_FRAGMENT_PENALTY = 0.70         # multiply match score by this factor


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
            "family_profile": _fetch_family_profile(conn, family),
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
    """
    Role-aware scoring multiplier.

    Design intent (Patch-1):
      - Same role match is strongly rewarded.
      - reactant ↔ product cross-match is heavily penalised (they are
        semantically opposite ends of a transformation).
      - agent ↔ reactant has a mild penalty (common to confuse solvent/base
        with substrate, but not catastrophic).
      - unknown roles get a neutral multiplier so single-SMILES queries
        are not accidentally penalised.
    """
    q = _norm_role(query_role)
    i = _norm_role(indexed_role)
    if q == "unknown" or i == "unknown":
        return 1.0
    if q == i:
        return 1.25          # same-role: +25% reward (was 1.15)
    if {q, i} == {"agent", "reactant"}:
        return 0.80          # mild penalty: agent/reactant confusion (was 0.95)
    if {q, i} == {"agent", "product"}:
        return 0.60          # moderate penalty: agent ≠ product
    # reactant ↔ product: opposite ends of the transformation — heavy penalty
    return 0.30              # was 0.88



def _small_fragment_penalty(smiles: Optional[str]) -> float:
    """Return a penalty multiplier for common small fragments (Patch-3).

    Molecules with ≤ SMALL_FRAGMENT_HA_THRESHOLD heavy atoms are likely
    solvents/reagents (AcOH, Et3N, EtOAc, ...) and provide little
    family-discriminating signal, so we down-weight them.
    Returns 1.0 (no penalty) for unknown / unparseable SMILES.
    """
    if not smiles or Chem is None:
        return 1.0
    try:
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            return 1.0
        ha = mol.GetNumHeavyAtoms()
        return SMALL_FRAGMENT_PENALTY if ha <= SMALL_FRAGMENT_HA_THRESHOLD else 1.0
    except Exception:
        return 1.0


def _append_component_hit(target: Dict[int, Dict[str, Any]], extract_id: int, payload: Dict[str, Any]) -> None:
    # Patch-3: apply small fragment penalty to the query molecule
    raw_score = float(payload.get("match_score") or 0.0)
    frag_factor = _small_fragment_penalty(payload.get("query_smiles"))
    adjusted_score = raw_score * frag_factor

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
    slot["match_score"] += adjusted_score
    slot["quality_tier"] = min(int(slot.get("quality_tier") or 99), int(payload["quality_tier"]))
    comps = slot.setdefault("matched_components", [])
    comps.append(
        {
            "query_role": payload.get("query_role"),
            "query_smiles": payload.get("query_smiles"),
            "matched_role": payload.get("matched_role"),
            "matched_smiles": payload.get("matched_smiles"),
            "match_type": payload.get("match_type"),
            "match_score": round(adjusted_score, 3),
            "frag_penalty": round(frag_factor, 2),
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

        # Patch-2: apply extract_kind weight to match_score
        extract_kind = (item.get("extract_kind") or "").strip().lower()
        kind_multiplier = EXTRACT_KIND_WEIGHTS.get(extract_kind, 1.0)
        item["match_score"] = float(item.get("match_score") or 0.0) * kind_multiplier
        item["extract_kind_weight"] = round(kind_multiplier, 2)
        item["match_score"] = round(item["match_score"], 3)

    # Patch-4: family diversity penalty — re-sort then penalise repeated families
    results.sort(key=lambda x: (-float(x.get("match_score") or 0.0), int(x.get("quality_tier") or 99), int(x["extract_id"])))
    family_seen: Dict[str, int] = {}
    FAMILY_REPEAT_FREE = 2        # first N hits from same family: no penalty
    FAMILY_REPEAT_DECAY = 0.70    # each subsequent hit from same family is multiplied by this
    for item in results:
        fam = (item.get("reaction_family_name") or "").strip().lower()
        if not fam:
            continue
        count = family_seen.get(fam, 0)
        if count >= FAMILY_REPEAT_FREE:
            decay = FAMILY_REPEAT_DECAY ** (count - FAMILY_REPEAT_FREE + 1)
            item["match_score"] = round(float(item["match_score"]) * decay, 3)
            item["family_diversity_decay"] = round(decay, 3)
        family_seen[fam] = count + 1

    # Final sort after all adjustments
    results.sort(key=lambda x: (-float(x.get("match_score") or 0.0), int(x.get("quality_tier") or 99), int(x["extract_id"])))

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
    if final:
        conn = _db_connect()
        try:
            profile_table = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reaction_family_patterns'").fetchone()
            fam_cache = {}
            if profile_table:
                for fam in {r.get("reaction_family_name") for r in final if r.get("reaction_family_name")}:
                    fam_cache[fam] = _fetch_family_profile(conn, fam) if fam else None
            for item in final:
                _naturalize_item(item, fam_cache.get(item.get("reaction_family_name")))
            families = [r.get("reaction_family_name") for r in final if r.get("reaction_family_name")]
            families = list(dict.fromkeys(families))[:5]
            if families and profile_table:
                payload["family_profiles"] = [fam_cache.get(f) for f in families if fam_cache.get(f)]
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
