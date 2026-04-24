from __future__ import annotations

import os
import math
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


# Patch-B1: reaction delta scoring feature patterns
_REACTION_FEATURE_SMARTS: Dict[str, str] = {
    "carbonyl": "[CX3]=[OX1]",
    "alcohol": "[OX2H][#6]",
    "carboxylic_acid": "[CX3](=O)[OX2H1]",
    "acid_chloride": "[CX3](=O)Cl",
    "ester": "[CX3](=O)O[#6]",
    "amide": "[NX3][CX3](=O)",
    "amine": "[NX3;H2,H1;!$(NC=O)]",
    "aryl_halide": "[c][F,Cl,Br,I]",
    "boron": "[B,b]",
    "nitro": "[$([N+](=O)[O-])]",
    "oxime": "[CX3]=[NX2][OX2H,OX1-]",
    "alkene": "C=C",
    "alkyne": "C#C",
    "ether": "[#6]-O-[#6]",
    "aromatic_ring": "a1aaaaa1",
}
_REACTION_FEATURE_MOLS = {k: Chem.MolFromSmarts(v) for k, v in _REACTION_FEATURE_SMARTS.items()} if Chem is not None else {}

def _count_reaction_features(smiles: Optional[str]) -> Dict[str, int]:
    counts = {k: 0 for k in _REACTION_FEATURE_SMARTS.keys()}
    counts["heavy_atoms"] = 0
    if not smiles or Chem is None:
        return counts
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return counts
    counts["heavy_atoms"] = mol.GetNumHeavyAtoms()
    for key, patt in _REACTION_FEATURE_MOLS.items():
        if patt is None:
            continue
        try:
            counts[key] = len(mol.GetSubstructMatches(patt))
        except Exception:
            counts[key] = 0
    # robust diazo fallback from text form
    diazo_txt = smiles.count('N+]=[N-') + smiles.count('=[N+]=[N-') + smiles.count('N=[N+]=[N-')
    counts["diazo"] = max(counts.get("diazo", 0), diazo_txt)
    return counts

def _merge_feature_counts(base: Dict[str, int], add: Dict[str, int]) -> None:
    for k, v in add.items():
        base[k] = int(base.get(k, 0)) + int(v or 0)

def _classify_single_smiles_as_query(smiles: str) -> Dict[str, Any]:
    """
    Single SMILES 쿼리용 FG 프로파일러 (Patch-S1 safe).

    반환값:
      {
        "inferred_role": "reactant" | "agent" | "ambiguous",
        "fg_profile": { feature: count, ... },
        "query_signals": { signal_name: bool, ... },
        "signal_notes": [str, ...],
      }

    설계 원칙:
      - reaction_delta는 계산하지 않는다 (delta 엔진 미사용).
      - "이 분자가 어떤 반응의 전구체일 수 있는가"만 판단한다.
      - inferred_role == "agent" 이면 모든 positive family signal을 끈다.
        (작은 조각이 부당하게 family를 boost하는 것을 막는다.)
      - query_signals는 _family_coarse_gate_adjustment()의
        precomputed_signals로 전달된다.
    """
    if Chem is None or not smiles:
        return {
            "inferred_role": "unknown",
            "fg_profile": {},
            "query_signals": {k: False for k in _COARSE_SIGNAL_LABELS_KO.keys()},
            "signal_notes": ["RDKit 미사용 — 신호 비활성화"],
        }

    fg = _count_reaction_features(smiles)
    ha = fg.get("heavy_atoms", 0)
    notes: List[str] = []

    # ── Step 1: role 추론 ──────────────────────────────────────────
    if ha <= SMALL_FRAGMENT_HA_THRESHOLD:
        inferred_role = "agent"
        notes.append(f"small fragment (HA={ha}) → agent/용매 추정")
    elif (fg.get("carboxylic_acid", 0) + fg.get("acid_chloride", 0)) >= 1:
        inferred_role = "reactant"
        notes.append("carboxylic acid / acid chloride → reactant 추정")
    elif fg.get("aryl_halide", 0) >= 1:
        inferred_role = "reactant"
        notes.append("aryl halide → reactant 추정 (coupling)")
    elif fg.get("oxime", 0) >= 1:
        inferred_role = "reactant"
        notes.append("oxime → reactant 추정 (Beckmann)")
    elif fg.get("alcohol", 0) >= 1 and fg.get("carbonyl", 0) == 0:
        inferred_role = "reactant"
        notes.append("alcohol (no carbonyl) → reactant 추정")
    elif fg.get("amine", 0) >= 1:
        inferred_role = "reactant"
        notes.append("amine → reactant 추정")
    elif fg.get("aromatic_ring", 0) >= 1 and fg.get("carbonyl", 0) >= 1:
        inferred_role = "reactant"
        notes.append("arene + carbonyl → reactant 추정")
    else:
        inferred_role = "ambiguous"
        notes.append("FG 조합 ambiguous → 약한 gate만 적용")

    # ── Step 2: signal 초기화 ──────────────────────────────────────
    signals: Dict[str, bool] = {k: False for k in _COARSE_SIGNAL_LABELS_KO.keys()}

    # ── Step 3: agent면 모든 positive signal OFF ──────────────────
    # agent/small fragment는 반응의 주역이 아니므로
    # 어떤 FG를 갖고 있더라도 family를 자극해선 안 된다.
    # coarse gate의 missing_need_factor만 작동하게 두는 것이 목적이다.
    if inferred_role == "agent":
        notes.append("agent 추정 → 모든 positive family signal 비활성화")
        notes.append("coarse gate: missing_need_factor만 적용됨")
        return {
            "inferred_role": inferred_role,
            "fg_profile": fg,
            "query_signals": signals,   # 전부 False
            "signal_notes": notes,
        }

    # ── Step 4: reactant / ambiguous 전용 signal 계산 ─────────────
    # delta(전후 변화)는 계산하지 않는다.
    # "이 FG가 존재한다" = "이 반응의 전구체일 수 있다"만 판단.

    # decarboxylation: 카복실산/산염화물 전구체
    if (fg.get("carboxylic_acid", 0) + fg.get("acid_chloride", 0)) >= 1:
        signals["decarboxylation"] = True

    # deoxygenation: 알코올 전구체 (ether/carbonyl 없을 때 — Barton-McCombie)
    if fg.get("alcohol", 0) >= 1 and fg.get("ether", 0) == 0 and fg.get("carbonyl", 0) == 0:
        signals["deoxygenation"] = True

    # coupling: aryl halide + (amine 또는 boron) 조합
    if fg.get("aryl_halide", 0) >= 1 and (fg.get("amine", 0) + fg.get("boron", 0)) >= 1:
        signals["coupling"] = True

    # oxime_rearrangement: oxime 전구체
    if fg.get("oxime", 0) >= 1:
        signals["oxime_rearrangement"] = True

    # multicomponent_condensation: carbonyl >= 2 + amine >= 1 + aromatic
    if fg.get("aromatic_ring", 0) >= 1 and fg.get("carbonyl", 0) >= 2 and fg.get("amine", 0) >= 1:
        signals["multicomponent_condensation"] = True

    # oxidation / reduction: 방향성 불명 → OFF
    signals["oxidation"] = False
    signals["reduction"] = False

    active = [k for k, v in signals.items() if v]
    if active:
        notes.append(f"활성 신호: {', '.join(active)}")

    return {
        "inferred_role": inferred_role,
        "fg_profile": fg,
        "query_signals": signals,
        "signal_notes": notes,
    }

def _reaction_delta_from_components(parsed: Optional[Dict[str, List[str]]]) -> Dict[str, Any]:
    keys = list(_REACTION_FEATURE_SMARTS.keys()) + ["heavy_atoms"]
    react = {k: 0 for k in keys}
    prod = {k: 0 for k in keys}
    if not parsed:
        return {"reactants": react, "products": prod, "delta": {k: 0 for k in keys}}
    for side in (parsed.get("reactants") or []) + (parsed.get("agents") or []):
        _merge_feature_counts(react, _count_reaction_features(side))
    for side in (parsed.get("products") or []):
        _merge_feature_counts(prod, _count_reaction_features(side))
    delta = {k: int(prod.get(k, 0)) - int(react.get(k, 0)) for k in keys}
    return {"reactants": react, "products": prod, "delta": delta}


_COARSE_SIGNAL_LABELS_KO: Dict[str, str] = {
    "ring_expansion": "고리 확장",
    "diazo_arene_combo": "diazo + arene 조합",
    "coupling": "커플링",
    "oxidation": "산화",
    "reduction": "환원",
    "dehydration": "탈수",
    "deoxygenation": "탈산소화",
    "decarboxylation": "탈카복실화",
    "ester_insertion_oxidation": "ester/lactone 삽입형 산화",
    "oxime_rearrangement": "oxime 재배열",
    "multicomponent_condensation": "다성분 축합",
    "carbohydrate_rearrangement": "당/다가산소 재배열",
}


# Step-2.5: coarse mismatch residual noise clipping
# If a family survives only with a very small score *and* a very low coarse gate multiplier,
# it is almost always cross-family noise rather than useful experimental guidance.
LOW_SCORE_MISMATCH_MAX_SCORE = 0.03
LOW_SCORE_MISMATCH_MAX_MULTIPLIER = 0.12

def _reaction_coarse_signals(reaction_delta: Optional[Dict[str, Any]]) -> Dict[str, bool]:
    if not reaction_delta:
        return {k: False for k in _COARSE_SIGNAL_LABELS_KO.keys()}
    r = reaction_delta.get("reactants", {}) or {}
    p = reaction_delta.get("products", {}) or {}
    d = reaction_delta.get("delta", {}) or {}

    signals: Dict[str, bool] = {}
    signals["diazo_arene_combo"] = (r.get("diazo", 0) >= 1 and r.get("aromatic_ring", 0) >= 1)

    oxygenated_reactant_load = (
        int(r.get("alcohol", 0) or 0)
        + int(r.get("ether", 0) or 0)
        + int(r.get("carbonyl", 0) or 0)
        + int(r.get("ester", 0) or 0)
    )

    signals["ring_expansion"] = bool(
        signals["diazo_arene_combo"]
        or (
            r.get("aromatic_ring", 0) >= 1
            and p.get("aromatic_ring", 0) >= 1
            and (p.get("heavy_atoms", 0) - r.get("heavy_atoms", 0)) >= -2
            and r.get("diazo", 0) >= 1
        )
    )
    signals["coupling"] = bool(
        r.get("aryl_halide", 0) >= 1
        and (r.get("amine", 0) + r.get("alcohol", 0) + r.get("boron", 0)) >= 1
    )
    signals["oxidation"] = bool(
        d.get("ester", 0) > 0
        or (d.get("carbonyl", 0) > 0 and d.get("alcohol", 0) <= 0)
    )
    signals["reduction"] = bool(
        d.get("carbonyl", 0) < 0
        and (d.get("alcohol", 0) > 0 or p.get("amine", 0) > r.get("amine", 0))
    )
    signals["dehydration"] = bool(d.get("alcohol", 0) < 0 and d.get("alkene", 0) > 0)
    signals["deoxygenation"] = bool(
        d.get("alcohol", 0) < 0
        and d.get("alkene", 0) <= 0
        and d.get("carbonyl", 0) <= 0
        and d.get("ester", 0) <= 0
    )
    signals["decarboxylation"] = bool(
        (r.get("carboxylic_acid", 0) + r.get("acid_chloride", 0)) >= 1
        and (
            d.get("carboxylic_acid", 0) < 0
            or d.get("acid_chloride", 0) < 0
            or d.get("carbonyl", 0) < 0
        )
    )
    signals["ester_insertion_oxidation"] = bool(r.get("carbonyl", 0) >= 1 and d.get("ester", 0) > 0)
    signals["oxime_rearrangement"] = bool(r.get("oxime", 0) >= 1)
    signals["multicomponent_condensation"] = bool(r.get("carbonyl", 0) >= 2 and r.get("amine", 0) >= 1)
    signals["carbohydrate_rearrangement"] = bool(
        oxygenated_reactant_load >= 4
        and r.get("amine", 0) >= 1
        and (r.get("alcohol", 0) >= 2 or r.get("ether", 0) >= 2)
    )
    return signals

def _active_reaction_types(signals: Dict[str, bool]) -> List[str]:
    return [name for name, active in signals.items() if active]

def _family_coarse_profile(family_name: Optional[str]) -> Optional[Dict[str, Any]]:
    if not family_name:
        return None
    fam = family_name.strip().lower()
    if not fam:
        return None

    if fam.startswith("buchner") or "buchner method of ring expansion" in fam:
        return {
            "need_any": ["ring_expansion", "diazo_arene_combo"],
            "boost_any": ["ring_expansion", "diazo_arene_combo"],
            "forbid_any": ["carbohydrate_rearrangement", "multicomponent_condensation", "dehydration", "deoxygenation", "decarboxylation", "coupling", "oxidation", "ester_insertion_oxidation", "oxime_rearrangement"],
            "missing_need_factor": 0.72,
            "forbidden_factor": 0.22,
            "boost_factor": 1.18,
        }
    if "amadori" in fam:
        return {
            "need_any": ["carbohydrate_rearrangement"],
            "boost_any": ["carbohydrate_rearrangement"],
            "forbid_any": ["ring_expansion", "diazo_arene_combo", "coupling", "dehydration", "deoxygenation", "decarboxylation", "oxidation", "reduction", "ester_insertion_oxidation", "oxime_rearrangement"],
            "missing_need_factor": 0.08,
            "forbidden_factor": 0.15,
            "boost_factor": 1.08,
        }
    if "buchwald-hartwig" in fam:
        return {
            "need_any": ["coupling"],
            "boost_any": ["coupling"],
            "forbid_any": ["ring_expansion", "carbohydrate_rearrangement", "dehydration", "deoxygenation"],
            "missing_need_factor": 0.40,
            "forbidden_factor": 0.35,
            "boost_factor": 1.15,
        }
    if "baeyer-villiger" in fam:
        return {
            "need_any": ["ester_insertion_oxidation", "oxidation"],
            "boost_any": ["ester_insertion_oxidation"],
            "forbid_any": ["reduction", "coupling", "dehydration", "deoxygenation", "ring_expansion", "diazo_arene_combo"],
            "missing_need_factor": 0.28,
            "forbidden_factor": 0.25,
            "boost_factor": 1.12,
        }
    if "beckmann" in fam:
        return {
            "need_any": ["oxime_rearrangement"],
            "boost_any": ["oxime_rearrangement"],
            "forbid_any": ["coupling", "ring_expansion", "multicomponent_condensation", "decarboxylation"],
            "missing_need_factor": 0.30,
            "forbidden_factor": 0.40,
            "boost_factor": 1.12,
        }
    if "barton radical decarboxylation" in fam:
        return {
            "need_any": ["decarboxylation"],
            "boost_any": ["decarboxylation"],
            "forbid_any": ["carbohydrate_rearrangement", "coupling", "multicomponent_condensation", "ester_insertion_oxidation"],
            "missing_need_factor": 0.30,
            "forbidden_factor": 0.38,
            "boost_factor": 1.10,
        }
    if "barton-mccombie" in fam:
        return {
            "need_any": ["deoxygenation"],
            "boost_any": ["deoxygenation"],
            "forbid_any": ["carbohydrate_rearrangement", "coupling", "multicomponent_condensation", "ester_insertion_oxidation"],
            "missing_need_factor": 0.35,
            "forbidden_factor": 0.40,
            "boost_factor": 1.10,
        }
    if "biginelli" in fam:
        return {
            "need_any": ["multicomponent_condensation"],
            "boost_any": ["multicomponent_condensation"],
            "forbid_any": ["ring_expansion", "deoxygenation", "dehydration", "reduction"],
            "missing_need_factor": 0.25,
            "forbidden_factor": 0.35,
            "boost_factor": 1.10,
        }
    if "burgess" in fam:
        return {
            "need_any": ["dehydration"],
            "boost_any": ["dehydration"],
            "forbid_any": ["ring_expansion", "carbohydrate_rearrangement", "coupling", "ester_insertion_oxidation", "reduction"],
            "missing_need_factor": 0.35,
            "forbidden_factor": 0.35,
            "boost_factor": 1.12,
        }
    if "chugaev" in fam:
        return {
            # Chugaev elimination should not win on generic radical / tin-hydride mechanism fragments.
            # Require a fairly strong dehydration / alkene-forming signal and penalize radical/decarboxylation families.
            "need_any": ["dehydration"],
            "boost_any": ["dehydration"],
            "forbid_any": [
                "deoxygenation", "decarboxylation", "coupling", "ring_expansion",
                "multicomponent_condensation", "ester_insertion_oxidation", "carbohydrate_rearrangement"
            ],
            "missing_need_factor": 0.18,
            "forbidden_factor": 0.28,
            "boost_factor": 1.12,
        }

    # Active rejected 8 families — explicit coarse penalties to stop benchmark hijack.
    _BUCHNER_FORBID = {
        "claisen condensation / claisen reaction",
        "horner-wadsworth-emmons olefination",
        "krapcho dealkoxycarbonylation",
        "michael addition reaction",
        "regitz diazo transfer",
    }
    if fam in _BUCHNER_FORBID:
        return {
            "need_any": [],
            "boost_any": [],
            "forbid_any": ["diazo_arene_combo", "ring_expansion"],
            "missing_need_factor": 1.0,
            "forbidden_factor": 0.10,
            "boost_factor": 1.0,
        }

    _BARTON_FORBID = {
        "enyne metathesis",
        "hofmann-loffler-freytag reaction",
        "mitsunobu reaction",
    }
    if fam in _BARTON_FORBID:
        return {
            "need_any": [],
            "boost_any": [],
            "forbid_any": ["decarboxylation", "deoxygenation"],
            "missing_need_factor": 1.0,
            "forbidden_factor": 0.10,
            "boost_factor": 1.0,
        }

    # ── 예외 봉인 (DB 유도로 오분류되는 케이스를 먼저 차단) ──────
    # Claisen Condensation: DB에서 family_class=rearrangement로 잘못 분류돼 있음
    # → sigmatropic gate를 타면 안 됨 → 범용 condensation profile로 강제
    if "claisen condensation" in fam or "claisen reaction" in fam:
        return {
            "need_any": [],
            "boost_any": [],
            "forbid_any": ["deoxygenation", "decarboxylation", "ring_expansion",
                           "diazo_arene_combo", "oxime_rearrangement"],
            "missing_need_factor": 1.0,
            "forbidden_factor": 0.38,
            "boost_factor": 1.0,
        }

    # Brook Rearrangement: transformation_type에 silyl이 있지만
    # DB에서 family_class=named_reaction이라 rearrangement 분기 진입이 불안정
    # → silyl migration gate로 강제
    if "brook rearrangement" in fam or "brook" in fam:
        return {
            "need_any": [],
            "boost_any": [],
            "forbid_any": ["decarboxylation", "deoxygenation", "coupling",
                           "multicomponent_condensation", "carbohydrate_rearrangement"],
            "missing_need_factor": 1.0,
            "forbidden_factor": 0.40,
            "boost_factor": 1.0,
        }

    # Chichibabin: family_class=named_reaction이지만 amination/coupling 계열
    # rearrangement 분기에 빠지지 않도록 봉인
    if "chichibabin" in fam:
        return {
            "need_any": [],
            "boost_any": [],
            "forbid_any": ["deoxygenation", "decarboxylation", "ring_expansion",
                           "ester_insertion_oxidation", "carbohydrate_rearrangement"],
            "missing_need_factor": 1.0,
            "forbidden_factor": 0.40,
            "boost_factor": 1.0,
        }

    # ── DB 메타데이터 기반 fallback (A-lite) ─────────────────────────
    # 하드코딩 9개에 매칭되지 않은 family는 DB에서 메타데이터를 읽어
    # coarse profile을 유도한다.
    # "전체 ontology 완성"이 아니라 "설명 필드에서 gate 신호를 추론"하는 방식.
    return _derive_coarse_profile_from_db(family_name)


# DB fallback 캐시 — DB 조회를 반복하지 않도록
_db_coarse_profile_cache: Dict[str, Optional[Dict[str, Any]]] = {}
_db_coarse_profile_cache_lock = threading.Lock()


def _derive_coarse_profile_from_db(family_name: str) -> Optional[Dict[str, Any]]:
    """
    DB의 reaction_family_patterns 메타데이터로부터 coarse profile을 유도한다.

    유도 규칙 (A-lite):
      family_class / transformation_type / key_reagents_clue / description_short를
      키워드 매칭으로 분석해서 need_any / forbid_any / boost_any를 결정한다.

    반환값이 None이면 coarse gate 미적용 (1.0).
    """
    cache_key = family_name.strip().lower()
    with _db_coarse_profile_cache_lock:
        if cache_key in _db_coarse_profile_cache:
            return _db_coarse_profile_cache[cache_key]

    result = _derive_coarse_profile_from_db_uncached(family_name)

    with _db_coarse_profile_cache_lock:
        _db_coarse_profile_cache[cache_key] = result
    return result


def _derive_coarse_profile_from_db_uncached(family_name: str) -> Optional[Dict[str, Any]]:
    if not DB_PATH.exists():
        return None
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        # ORDER BY: 구조화된 class를 우선 선택
        # coupling/oxidation/reduction 등 명확한 class > rearrangement > named_reaction/named reaction > other/synthesis
        row = conn.execute(
            """
            SELECT family_class, transformation_type, mechanism_type,
                   reactant_pattern_text, product_pattern_text,
                   key_reagents_clue, description_short
            FROM reaction_family_patterns
            WHERE lower(family_name) = lower(?)
               OR lower(family_name_norm) = lower(?)
            ORDER BY
                CASE
                    WHEN lower(family_class) IN ('coupling','cross-coupling','cross_coupling',
                                                 'oxidation','reduction','elimination',
                                                 'condensation','cycloaddition','annulation',
                                                 'metathesis','olefination','fragmentation',
                                                 'sigmatropic rearrangement') THEN 1
                    WHEN lower(family_class) = 'rearrangement' THEN 2
                    WHEN lower(family_class) IN ('named_reaction','named reaction') THEN 3
                    ELSE 4
                END
            LIMIT 1
            """,
            (family_name, family_name),
        ).fetchone()
        conn.close()
    except Exception:
        return None

    if not row:
        return None

    fc   = str(row["family_class"] or "").lower().strip()
    tt   = str(row["transformation_type"] or "").lower()
    mt   = str(row["mechanism_type"] or "").lower()
    rpt  = str(row["reactant_pattern_text"] or "").lower()
    ppt  = str(row["product_pattern_text"] or "").lower()
    krc  = str(row["key_reagents_clue"] or "").lower()
    desc = str(row["description_short"] or "").lower()
    combined = " ".join([fc, tt, mt, rpt, ppt, krc, desc])

    # family_class 정규화 — DB에 여러 표기 혼재
    def _norm_fc(s: str) -> str:
        s = s.replace("-", "_").replace(" ", "_")
        if s in ("named_reaction", "named_reactions"):
            return "named_reaction"
        if "coupling" in s or "cross_coupling" in s:
            return "coupling"
        if "sigmatropic" in s or s == "rearrangement":
            return "rearrangement"
        if "oxidation" in s:
            return "oxidation"
        if "reduction" in s:
            return "reduction"
        if "elimination" in s or "olefination" in s:
            return "elimination"
        if "condensation" in s:
            return "condensation"
        if "cycloaddition" in s or "annulation" in s or "cyclization" in s:
            return "cycloaddition"
        return s

    fc_norm = _norm_fc(fc)

    need_any:   List[str] = []
    boost_any:  List[str] = []
    forbid_any: List[str] = []
    missing_need_factor = 0.45
    forbidden_factor    = 0.40
    boost_factor        = 1.08

    # ── coupling 계열 ──────────────────────────────────────────────
    if fc_norm == "coupling" or "coupling" in tt:
        need_any  = ["coupling"]
        boost_any = ["coupling"]
        forbid_any = ["decarboxylation", "deoxygenation", "carbohydrate_rearrangement",
                      "ring_expansion", "oxime_rearrangement"]
        missing_need_factor = 0.40
        forbidden_factor    = 0.35
        boost_factor        = 1.10

    # ── rearrangement 계열 ────────────────────────────────────────
    elif fc_norm in ("rearrangement", "named_reaction") or "rearrangement" in tt:
        if "silyl" in tt or "silicon" in tt or "silyl" in mt:
            forbid_any = ["decarboxylation", "deoxygenation", "coupling",
                          "multicomponent_condensation"]
            missing_need_factor = 0.55
            forbidden_factor    = 0.40
        elif "condensation" in desc and "rearrangement" not in desc:
            need_any   = ["multicomponent_condensation"]
            boost_any  = ["multicomponent_condensation"]
            forbid_any = ["deoxygenation", "decarboxylation", "ring_expansion",
                          "diazo_arene_combo"]
            missing_need_factor = 0.40
            forbidden_factor    = 0.38
        elif "sigmatropic" in mt or "[3,3]" in combined or "allyl" in combined:
            forbid_any = ["decarboxylation", "deoxygenation", "coupling",
                          "multicomponent_condensation", "ring_expansion"]
            missing_need_factor = 0.50
            forbidden_factor    = 0.38
        elif "diketone" in combined or "alpha-hydroxy" in combined or "benzilic" in combined:
            need_any   = ["oxidation"]
            boost_any  = ["oxidation", "ester_insertion_oxidation"]
            forbid_any = ["decarboxylation", "deoxygenation", "coupling",
                          "ring_expansion", "multicomponent_condensation"]
            missing_need_factor = 0.40
            forbidden_factor    = 0.35
        else:
            forbid_any = ["decarboxylation", "deoxygenation", "coupling",
                          "multicomponent_condensation"]
            missing_need_factor = 0.50
            forbidden_factor    = 0.40

    # ── oxidation 계열 ────────────────────────────────────────────
    elif fc_norm == "oxidation" or "oxidation" in tt:
        need_any   = ["oxidation", "ester_insertion_oxidation"]
        boost_any  = ["oxidation"]
        forbid_any = ["deoxygenation", "reduction", "decarboxylation",
                      "ring_expansion", "multicomponent_condensation"]
        missing_need_factor = 0.40
        forbidden_factor    = 0.35
        boost_factor        = 1.08

    # ── reduction 계열 ────────────────────────────────────────────
    elif fc_norm == "reduction" or "reduction" in tt:
        need_any   = ["reduction"]
        boost_any  = ["reduction"]
        forbid_any = ["oxidation", "ester_insertion_oxidation", "decarboxylation",
                      "ring_expansion", "coupling"]
        missing_need_factor = 0.40
        forbidden_factor    = 0.35
        boost_factor        = 1.08

    # ── condensation 계열 ─────────────────────────────────────────
    elif fc_norm == "condensation" or "condensation" in tt:
        need_any   = ["multicomponent_condensation"]
        boost_any  = ["multicomponent_condensation"]
        forbid_any = ["deoxygenation", "decarboxylation", "ring_expansion",
                      "diazo_arene_combo"]
        missing_need_factor = 0.40
        forbidden_factor    = 0.38

    # ── elimination / dehydration 계열 ───────────────────────────
    elif fc_norm == "elimination" or "dehydration" in tt or "elimination" in tt:
        need_any   = ["dehydration"]
        boost_any  = ["dehydration"]
        forbid_any = ["reduction", "coupling", "ring_expansion",
                      "multicomponent_condensation"]
        missing_need_factor = 0.40
        forbidden_factor    = 0.38

    # ── amination 계열 ───────────────────────────────────────────
    elif "aminat" in tt or "aminat" in desc or "amination" in krc:
        need_any   = ["coupling"]
        boost_any  = ["coupling"]
        forbid_any = ["deoxygenation", "decarboxylation", "ring_expansion",
                      "ester_insertion_oxidation"]
        missing_need_factor = 0.45
        forbidden_factor    = 0.40

    # ── cycloaddition / annulation 계열 ──────────────────────────
    elif fc_norm == "cycloaddition" or "cycloaddition" in tt:
        forbid_any = ["decarboxylation", "deoxygenation", "coupling",
                      "multicomponent_condensation"]
        missing_need_factor = 0.55
        forbidden_factor    = 0.40

    else:
        return None

    if not need_any:
        missing_need_factor = 1.0

    return {
        "need_any": need_any,
        "boost_any": boost_any,
        "forbid_any": forbid_any,
        "missing_need_factor": missing_need_factor,
        "forbidden_factor": forbidden_factor,
        "boost_factor": boost_factor,
        "_source": "db_derived",
    }

def _family_coarse_gate_adjustment(
    family_name: Optional[str],
    reaction_delta: Optional[Dict[str, Any]],
    precomputed_signals: Optional[Dict[str, bool]] = None,
) -> Tuple[float, List[str], Dict[str, bool]]:
    """
    family와 반응 신호를 비교해 coarse gate 배수를 반환한다.

    signals 공급 방식:
      1. reaction query  → reaction_delta 로부터 _reaction_coarse_signals() 계산
      2. single SMILES   → precomputed_signals (FG 존재성 기반, delta 없음)
      3. 둘 다 없음      → gate 미적용 (1.0)
    """
    if not family_name:
        return 1.0, [], {}

    if precomputed_signals is not None:
        signals = precomputed_signals
    elif reaction_delta is not None:
        signals = _reaction_coarse_signals(reaction_delta)
    else:
        return 1.0, [], {}

    profile = _family_coarse_profile(family_name)
    if not profile:
        return 1.0, [], signals

    mult = 1.0
    notes: List[str] = []

    need_any = profile.get("need_any") or []
    if need_any and not any(signals.get(sig, False) for sig in need_any):
        mult *= float(profile.get("missing_need_factor", 0.55))
        missing_desc = " / ".join(_COARSE_SIGNAL_LABELS_KO.get(sig, sig) for sig in need_any[:3])
        notes.append(f"반응 타입 게이트 미충족: {missing_desc}")

    forbid_hits = [sig for sig in (profile.get("forbid_any") or []) if signals.get(sig, False)]
    if forbid_hits:
        factor = float(profile.get("forbidden_factor", 0.45))
        for idx, sig in enumerate(forbid_hits[:3]):
            mult *= factor if idx == 0 else max(0.60, factor + 0.20)
            notes.append(f"반응 타입 불일치: {_COARSE_SIGNAL_LABELS_KO.get(sig, sig)}")

    boost_hits = [sig for sig in (profile.get("boost_any") or []) if signals.get(sig, False)]
    if boost_hits:
        mult *= float(profile.get("boost_factor", 1.10))
        if notes is not None:
            notes.append(f"반응 타입 적합: {_COARSE_SIGNAL_LABELS_KO.get(boost_hits[0], boost_hits[0])}")

    mult = max(0.03, min(mult, 2.0))
    return mult, notes, signals



def _should_prune_low_confidence_mismatch(item: Dict[str, Any], query_mode: str) -> bool:
    """
    Drop low-confidence cross-family residue after coarse gating.

    Patch-S2: structure / mixture 모드로 확장.
    - reaction 모드: 기존 기준 유지 (strict — coarse gate notes 필수)
    - structure 모드: coarse gate가 작동했을 때만 pruning
      (coarse gate가 없는 경우는 건드리지 않음)
    - mixture 모드: structure와 동일 기준
    """
    fam = item.get("reaction_family_name")
    if not fam:
        return False
    score = float(item.get("match_score") or 0.0)
    coarse = float(item.get("coarse_gate_multiplier") or 1.0)
    notes = item.get("coarse_gate_notes") or []

    if query_mode == "reaction":
        # 기존 strict 기준
        if score >= LOW_SCORE_MISMATCH_MAX_SCORE:
            return False
        if coarse > LOW_SCORE_MISMATCH_MAX_MULTIPLIER:
            return False
        if not notes:
            return False
        return True

    if query_mode in ("structure", "mixture"):
        # coarse gate가 작동하지 않은 경우는 pruning 하지 않음
        # (pseudo_reaction_components가 없으면 coarse_gate_multiplier == 1.0)
        if coarse >= 1.0:
            return False
        # 점수 기준은 reaction보다 관대하게 (3배)
        if score >= LOW_SCORE_MISMATCH_MAX_SCORE * 3:
            return False
        if coarse > LOW_SCORE_MISMATCH_MAX_MULTIPLIER * 2:
            return False
        if not notes:
            return False
        return True

    return False

def _family_delta_adjustment(family_name: Optional[str], reaction_delta: Optional[Dict[str, Any]]) -> Tuple[float, List[str]]:
    if not family_name or not reaction_delta:
        return 1.0, []
    fam = family_name.lower()
    r = reaction_delta.get("reactants", {})
    p = reaction_delta.get("products", {})
    d = reaction_delta.get("delta", {})
    mult = 1.0
    notes: List[str] = []

    def penalize(f: float, note: str):
        nonlocal mult
        mult *= f
        notes.append(note)

    def boost(f: float, note: str):
        nonlocal mult
        mult *= f
        notes.append(note)

    if "barton radical decarboxylation" in fam:
        if (r.get("carboxylic_acid", 0) + r.get("acid_chloride", 0)) < 1:
            penalize(0.20, "산/산염화물 전구체 부족")
        if p.get("carboxylic_acid", 0) + p.get("acid_chloride", 0) > 0:
            penalize(0.40, "생성물에 산/산염화물 유지")
        if d.get("carbonyl", 0) < 0:
            boost(1.10, "carbonyl 감소")
    elif "barton-mccombie" in fam:
        if r.get("alcohol", 0) < 1:
            penalize(0.20, "알코올 전구체 부족")
        if d.get("alcohol", 0) >= 0:
            penalize(0.30, "deoxygenation 방향 불일치")
        else:
            boost(1.10, "알코올 감소")
    elif "baeyer-villiger" in fam:
        if r.get("carbonyl", 0) < 1:
            penalize(0.25, "케톤/카보닐 전구체 부족")
        if d.get("ester", 0) <= 0:
            penalize(0.35, "ester/lactone 증가 신호 부족")
        else:
            boost(1.20, "ester 증가")
        # --- Phase 3c-a BV-strengthening start (20260421_092729) ---
        # BV oxidizes a ketone/aldehyde to an ester; macrolactonization (Yamaguchi,
        # Keck, Corey-Nicolaou) also increases ester but *consumes* a pre-existing
        # carboxylic acid. Criegee oxidation cleaves a vicinal diol. Neither is BV.
        if r.get("carboxylic_acid", 0) >= 1 and d.get("carboxylic_acid", 0) < 0 and d.get("ester", 0) > 0:
            penalize(0.40, "macrolactonization 신호 — BV 가능성 하락")
        if r.get("alcohol", 0) >= 2 and d.get("alcohol", 0) < 0 and d.get("carbonyl", 0) > 0:
            penalize(0.55, "vicinal diol 개열 — Criegee 등 경합")
        # --- end Phase 3c-a BV-strengthening ---
    elif "beckmann" in fam:
        if r.get("oxime", 0) < 1:
            penalize(0.20, "oxime 전구체 부족")
        if d.get("amide", 0) > 0:
            boost(1.15, "amide 증가")
    elif "buchwald-hartwig" in fam:
        if r.get("aryl_halide", 0) < 1:
            penalize(0.25, "aryl halide 전구체 부족")
        if (r.get("amine", 0) + r.get("alcohol", 0)) < 1:
            penalize(0.35, "amine/alcohol coupling partner 부족")
        if r.get("aryl_halide", 0) >= 1 and (r.get("amine", 0) + r.get("alcohol", 0)) >= 1:
            boost(1.15, "coupling partner 조합 적합")
    elif fam.startswith('buchner') or 'buchner method of ring expansion' in fam:
        if r.get("diazo", 0) < 1:
            penalize(0.20, "diazo 전구체 부족")
        if r.get("aromatic_ring", 0) < 1:
            penalize(0.35, "arene 전구체 부족")
        if r.get("diazo", 0) >= 1 and r.get("aromatic_ring", 0) >= 1:
            boost(1.25, "diazo + arene 조합 적합")
    elif "amadori" in fam:
        if r.get("carbonyl", 0) < 1 or r.get("amine", 0) < 1:
            penalize(0.30, "carbonyl/amine 재배열 전구체 부족")
    elif "biginelli" in fam:
        if r.get("carbonyl", 0) < 2 or r.get("amine", 0) < 1:
            penalize(0.25, "다성분 축합 전구체 부족")
    elif "burgess" in fam:
        if r.get("alcohol", 0) < 1 and r.get("amide", 0) < 1:
            penalize(0.30, "탈수 대상 작용기 부족")
        if d.get("alkene", 0) > 0:
            boost(1.10, "alkene 증가")
    elif "chugaev" in fam:
        # Chugaev is a thermolytic xanthate elimination; exact radical/tin-hydride fragments should not outrank Barton families.
        if r.get("alcohol", 0) < 1 and r.get("ether", 0) < 1:
            penalize(0.22, "제거 대상 알코올/잔테이트 전구체 부족")
        if d.get("alkene", 0) <= 0:
            penalize(0.22, "alkene 생성 신호 부족")
        else:
            boost(1.10, "alkene 증가")
        if d.get("alcohol", 0) >= 0 and d.get("ether", 0) >= 0:
            penalize(0.35, "제거반응 방향 불일치")
    # --- Phase 3c-a pattern guards start (20260421_092729) ---
    # Family-specific delta guards added by Phase 3c-a to reduce admission pattern
    # bleed. Each branch is a defensive penalty (×≤1.0) for mis-match signatures;
    # none can boost a greedy family above its natural fingerprint score.
    elif "heck reaction" in fam or fam.startswith("heck "):
        # Heck: Pd-catalyzed coupling of aryl/vinyl halide with an olefin substrate.
        if r.get("aryl_halide", 0) < 1:
            penalize(0.30, "Heck: aryl halide 전구체 부족")
        if r.get("alkene", 0) < 1 and d.get("alkene", 0) <= 0:
            penalize(0.35, "Heck: alkene 전구체/생성물 부족")
        if r.get("alkyne", 0) >= 1:
            penalize(0.60, "Heck: alkyne 존재 — Sonogashira 등 경합")
        if r.get("boron", 0) >= 1:
            penalize(0.60, "Heck: boron 존재 — Suzuki 등 경합")
    # --- end Phase 3c-a pattern guards ---

    mult = max(0.05, min(mult, 2.0))
    return mult, notes


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
        family_profile = _fetch_family_profile(conn, family) if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reaction_family_patterns'").fetchone() else None
        results = [
            {
                **dict(row),
                "match_type": "family_text",
                "role": "family",
                "match_score": 0.0,
                "matched_smiles": None,
                "quality_tier": 3,
            }
            for row in rows
        ]
        for item in results:
            _naturalize_item(item, family_profile)
        return {
            "query_mode": "family",
            "query_family": family,
            "direct_count": 0,
            "generic_count": 0,
            "family_count": len(rows),
            "family_profile": family_profile,
            "results": results,
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



def _result_sort_key(item: Dict[str, Any]) -> Tuple[float, int, int]:
    return (
        -float(item.get("match_score") or 0.0),
        int(item.get("quality_tier") or 99),
        int(item.get("extract_id") or 0),
    )


def _family_group_key(item: Dict[str, Any]) -> str:
    fam = str(item.get("reaction_family_name") or "").strip().lower()
    if fam:
        return f"family::{fam}"
    return f"extract::{int(item.get('extract_id') or 0)}"


def _family_evidence_preview(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "extract_id": item.get("extract_id"),
        "match_score": round(float(item.get("match_score") or 0.0), 3),
        "extract_kind": item.get("extract_kind"),
        "page_no": item.get("page_no"),
        "source_zip": item.get("source_zip"),
        "image_filename": item.get("image_filename"),
        "transformation_text": item.get("transformation_text"),
        "reactants_text": item.get("reactants_text"),
        "products_text": item.get("products_text"),
        "matched_components": list(item.get("matched_components") or []),
    }


def _aggregate_results_by_family(results: List[Dict[str, Any]], top_k: int) -> Tuple[List[Dict[str, Any]], int]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    ordered_keys: List[str] = []
    for item in sorted(results, key=_result_sort_key):
        key = _family_group_key(item)
        if key not in groups:
            groups[key] = []
            ordered_keys.append(key)
        groups[key].append(item)

    aggregated: List[Dict[str, Any]] = []
    collapsed_evidence_count = 0
    member_weights = [1.0, 0.35, 0.20, 0.12, 0.08]

    for key in ordered_keys:
        members = sorted(groups[key], key=_result_sort_key)
        rep = dict(members[0])
        rep["representative_extract_id"] = rep.get("extract_id")
        rep["representative_match_score"] = round(float(rep.get("match_score") or 0.0), 3)

        previews = [_family_evidence_preview(m) for m in members[:6]]
        rep["family_evidence_items"] = previews
        rep["family_member_extract_ids"] = [m.get("extract_id") for m in members]
        rep["family_hit_count"] = len(members)
        rep["additional_evidence_count"] = max(0, len(members) - 1)
        rep["family_group_key"] = key
        rep["family_grouped"] = True
        if rep["additional_evidence_count"]:
            collapsed_evidence_count += rep["additional_evidence_count"]

        aggregate_score = 0.0
        for idx, member in enumerate(members):
            weight = member_weights[idx] if idx < len(member_weights) else 0.05
            aggregate_score += float(member.get("match_score") or 0.0) * weight
        base_score = float(members[0].get("match_score") or 0.0)
        cap_factor = 1.0 if len(members) <= 1 else 1.35
        rep["match_score"] = round(min(aggregate_score, base_score * cap_factor), 3)
        if len(members) > 1:
            rep["family_aggregation_boost"] = round(rep["match_score"] / max(base_score, 1e-9), 3)

        aggregated.append(rep)

    aggregated.sort(key=_result_sort_key)
    return aggregated[:top_k], collapsed_evidence_count


def _finalize_results(hit_map: Dict[int, Dict[str, Any]], req: StructureEvidenceRequest, query_mode: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    candidate_limit = max(req.top_k * 6, 48)
    raw_results = list(hit_map.values())
    raw_results.sort(key=_result_sort_key)
    raw_results = raw_results[:candidate_limit]

    reaction_delta = None
    query_type_signals: Dict[str, bool] = {}

    if extra and isinstance(extra.get("reaction_components"), dict):
        # ── reaction query 경로: delta + coarse gate + delta_adjustment 전부 사용 ──
        reaction_delta = _reaction_delta_from_components(extra.get("reaction_components"))
        query_type_signals = _reaction_coarse_signals(reaction_delta)

    elif extra and isinstance(extra.get("single_smiles_fg_signals"), dict):
        # ── single SMILES 경로: FG 존재성 신호만 사용, delta는 계산하지 않음 ──
        # reaction_delta = None 유지 → _family_delta_adjustment()는 건너뜀
        query_type_signals = extra["single_smiles_fg_signals"]

    details = _fetch_extract_details([r["extract_id"] for r in raw_results])
    for item in raw_results:
        detail = details.get(item["extract_id"])
        if detail:
            item.update(detail)

        delta_multiplier, delta_notes = _family_delta_adjustment(item.get("reaction_family_name"), reaction_delta)
        item["match_score"] = float(item.get("match_score") or 0.0) * delta_multiplier
        item["delta_multiplier"] = round(delta_multiplier, 3)
        if delta_notes:
            item["delta_notes"] = delta_notes

        # single SMILES 경로: precomputed_signals로 coarse gate 적용
        # reaction 경로: reaction_delta로 coarse gate 적용 (기존 동작 유지)
        single_fg_signals = (extra or {}).get("single_smiles_fg_signals") if reaction_delta is None else None
        coarse_multiplier, coarse_notes, _ = _family_coarse_gate_adjustment(
            item.get("reaction_family_name"),
            reaction_delta,
            precomputed_signals=single_fg_signals,
        )
        item["match_score"] = float(item.get("match_score") or 0.0) * coarse_multiplier
        item["coarse_gate_multiplier"] = round(coarse_multiplier, 3)
        if coarse_notes:
            item["coarse_gate_notes"] = coarse_notes

        extract_kind = (item.get("extract_kind") or "").strip().lower()
        kind_multiplier = EXTRACT_KIND_WEIGHTS.get(extract_kind, 1.0)
        item["match_score"] = float(item.get("match_score") or 0.0) * kind_multiplier
        item["extract_kind_weight"] = round(kind_multiplier, 2)
        item["match_score"] = round(item["match_score"], 3)

    raw_results.sort(key=_result_sort_key)
    pruned_results: List[Dict[str, Any]] = []
    pruned_mismatch_count = 0
    for item in raw_results:
        if _should_prune_low_confidence_mismatch(item, query_mode=query_mode):
            pruned_mismatch_count += 1
            continue
        pruned_results.append(item)

    raw_results = pruned_results
    final, collapsed_evidence_count = _aggregate_results_by_family(raw_results, top_k=req.top_k)

    conn = _db_connect()
    try:
        has_family_patterns = bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reaction_family_patterns'").fetchone())
        family_profile_cache: Dict[str, Optional[Dict[str, Any]]] = {}
        for item in final:
            fam = item.get("reaction_family_name")
            profile: Optional[Dict[str, Any]] = None
            if has_family_patterns and fam:
                key = str(fam).lower()
                if key not in family_profile_cache:
                    family_profile_cache[key] = _fetch_family_profile(conn, fam)
                profile = family_profile_cache.get(key)
            _naturalize_item(item, profile)
    finally:
        conn.close()

    payload = {
        "query_mode": query_mode,
        "direct_count": len([r for r in final if r.get("quality_tier") == 1]),
        "generic_count": len([r for r in final if r.get("quality_tier") == 2]),
        "family_count": 0,
        "family_group_count": len(final),
        "collapsed_evidence_count": collapsed_evidence_count,
        "raw_candidate_count": len(raw_results),
        "pruned_mismatch_count": pruned_mismatch_count,
        "results": final,
    }

    # Patch-S3: no_confident_hit 판정
    # top result 점수 기준으로 "신뢰할 만한 hit가 없다"를 명시적으로 표시.
    # 프론트엔드는 이 플래그를 보고 경고 배너를 표시한다.
    top_score = float(final[0].get("match_score") or 0.0) if final else 0.0

    NO_CONFIDENT_HIT_THRESHOLD = 0.45   # '중간' 이상 기준과 동일
    no_confident_hit = (not final) or (top_score < NO_CONFIDENT_HIT_THRESHOLD)
    payload["no_confident_hit"] = no_confident_hit
    payload["top_score"] = round(top_score, 3)

    # single SMILES 전용: inferred_role, active_signals 표시
    if query_mode == "structure" and extra and "single_smiles_profile" in extra:
        payload["single_smiles_profile"] = extra.pop("single_smiles_profile")
    if final:
        conn = _db_connect()
        try:
            families = [r.get("reaction_family_name") for r in final if r.get("reaction_family_name")]
            families = list(dict.fromkeys(families))[:3]
            if families and conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reaction_family_patterns'").fetchone():
                payload["family_profiles"] = [p for p in (_fetch_family_profile(conn, fam) for fam in families) if p]
        finally:
            conn.close()
    if query_type_signals:
        payload["query_reaction_types"] = _active_reaction_types(query_type_signals)
        payload["query_reaction_types_ko"] = [_COARSE_SIGNAL_LABELS_KO.get(x, x) for x in payload["query_reaction_types"]]
        payload["query_reaction_type_signals"] = query_type_signals
    if reaction_delta:
        payload["reaction_delta_summary"] = reaction_delta
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

    # Patch-S1 (safe): single SMILES FG 프로파일링
    # pseudo_reaction_components를 만들지 않는다 — delta 엔진을 태우지 않음.
    # _finalize_results()는 single_smiles_fg_signals가 있으면
    # coarse gate만 적용하고 delta_adjustment는 건너뛴다.
    single_profile = _classify_single_smiles_as_query(q["smiles"])

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

    return _finalize_results(
        hit_map,
        req=req,
        query_mode="structure",
        extra={
            "query_smiles": q["smiles"],
            # reaction_components는 넘기지 않는다 → delta 엔진 미사용
            "single_smiles_fg_signals": single_profile["query_signals"],  # coarse gate 전용
            "single_smiles_profile": {
                "inferred_role": single_profile["inferred_role"],
                "signal_notes": single_profile["signal_notes"],
                "active_signals": [k for k, v in single_profile["query_signals"].items() if v],
            },
        },
    )



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
