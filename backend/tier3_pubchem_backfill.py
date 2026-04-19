"""
tier3_pubchem_backfill.py  (v2 final — 2026-04-12)
====================================================
extract_molecules tier3 (smiles=NULL, queryable=0) 중
명확한 화학명으로 판단되는 normalized_text를 PubChem API로 조회해서
SMILES를 얻어오면 quality_tier=1, queryable=1로 UPDATE.

개선 사항:
  1. 필터 대폭 강화 — 모호 표현/조건/R-group/플레이스홀더 제거
  2. 순수 SMILES 문자열은 RDKit 직접 파싱으로 처리 (PubChem 불필요)
  3. dry-run에서 projected 전후 수치 별도 출력
  4. normalized_text 원문을 note_text에 보존 (provenance 추적)
  5. 429/503 backoff 재시도 (최대 3회)
  6. role_confidence = 0.75 (vision promote의 0.92보다 낮게)

실행:
    cd backend/
    python tier3_pubchem_backfill.py [--db app/labint.db] [--dry-run] [--delay 0.35]
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("pubchem_backfill")

# ── RDKit ─────────────────────────────────────────────────────────────────────
try:
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")
except ImportError:
    log.error("RDKit이 필요합니다. conda activate chemlens 후 실행하세요.")
    sys.exit(1)

try:
    import requests
except ImportError:
    log.error("requests가 필요합니다. pip install requests")
    sys.exit(1)

# ── 상수 ─────────────────────────────────────────────────────────────────────
STRUCTURE_SOURCE_PUBCHEM = "pubchem_name_lookup"
STRUCTURE_SOURCE_RDKIT   = "rdkit_direct_parse"
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
PUBCHEM_URL = (
    "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/"
    "{name}/property/IsomericSMILES/JSON"
)

# ── 필터 ─────────────────────────────────────────────────────────────────────
# 이 단어/구절이면 무조건 제외
SKIP_EXACT: set[str] = {
    # 플레이스홀더/역할어
    "reactant","product","reagent","solvent","intermediate","catalyst",
    "substrate","electrophile","nucleophile","base","acid","heat","water",
    "alkene","alkyne","ketone","aldehyde","alcohol","amine","ester","alkyl",
    "aryl","cyclic","linear","bicyclic","imide","imine","enol","enone",
    "diene","dienophile","acyloin","Acyloin","enediyne","nitrone","isomers",
    "oxidant","carbanion","carbenoid","biradical","diradical",
    "Reactant","Product","Reagent","Solvent","Intermediate","Catalyst",
    "Alkene","Alkane","Alkyne","Benzene","Nitrile","Ketone","Electrophile",
    "Heterocycle","Aminopyridine","Halopyridine","Haloquinoline",
    # 일반 용매
    "toluene","benzene","pyridine","methanol","ethanol","acetone",
    "diethyl ether","THF","DMF","DMSO","acetonitrile","hexane","hexanes",
    "chloroform","DCM","Et2O","MeOH","EtOH","EtOAc","xylene","dioxane",
    "diglyme","formamide","carbamate","quinoline","indole","pyrrole","oxime",
    "Raney Ni","Raney Ni.","DMAP","DMAP (cat.)","stir","steps","equiv","Base",
    # 설명적 표현
    "Aryl amine","Aryl ether","Aryl fluoride","aryl amine","aryl ether",
    "carboxylic acid","Nitrogen heterocycle","Bicyclic diketone","benzenoid diradical",
    "RCM catalyst","metal catalyst","Grubbs catalyst","acid or base",
    "H2O or Lewis acid","strong base","organic solvent","liquid NH3",
    "A molecular sieves","Na (metal)","molten sodium metal",
    "cyclic olefin","Substituted alkene","Exocyclic alkene","Aromatic product",
    "Trialkylborane","monoalkylborane","dialkylborane",
    "Phosphine oxide","Phosphonate ester","Phosphinic acid ester",
    "phosphonous acid ester","phosphinous acid ester","Dialkyl phosphonate",
    "Elimination product","cyclized product","Aminopyridine",
    "enol","enolate","dianion","zinc carbenoid",
    # 시약/일반명 — 반응 evidence 아님 또는 구조 ambiguity 높음
    "diazoketone","Isonitrile","Lindlar's catalyst","Burgess reagent",
}

# 이 패턴이 있으면 제외
SKIP_RE: list[re.Pattern] = [
    # R-group/placeholder
    re.compile(r"\bR\d*\b(?!\s*=)|\bAr\b(?!\w)|\bX\b(?!\w)|\bZ\b(?!\w)|\bLG\b|\bNu\b|\bEWG\b|\bEDG\b"),
    re.compile(r";"),                                       # 복합 혼합물 (세미콜론)
    re.compile(r"\d+\s*(equiv|mol%|°C|min|hr|h\b|wt%|mmol|mL|\s*%\s*\w)"),
    re.compile(r"^(heat|hv|Δ|RT|reflux|\d|:|°|\[|%)", re.I),
    # 반응 중간체/상태
    re.compile(r"\b(enolate|dianion|trianion|carbene|ylide|radical|biradical|zwitterion)\b", re.I),
    re.compile(r"\b(skeleton|system|framework|moiety|motif|adduct|conjugate|tautomer)\b", re.I),
    re.compile(r"\b(intermediate|precursor|transition state)\b", re.I),
    re.compile(r"\b(mixture|polymer|oligomer|byproduct|by-product)\b", re.I),
    re.compile(r"\b(derivative|analog|analogue|substrate|starting material)\b", re.I),
    re.compile(r"\b(salt\b|complex\b|species\b|fragment\b)\b", re.I),
    re.compile(r"\b(UV|light|microwave|heating|irradiation|catalysis|work.?up)\b", re.I),
    re.compile(r"\b(then|followed|step|excess|cat\.|cat\b)\b", re.I),
    re.compile(r"\b(favored|unfavored|major|minor|desired|putative|novel|rearranged)\b", re.I),
    re.compile(r"\b(protected|unprotected|functionalized|substituted|activated)\b", re.I),
    re.compile(r"\b(product|precursor|shown on page|from page)\b", re.I),
    re.compile(r"[,]\s*(or|and)\s+[a-z]", re.I),          # "acid or base"
    re.compile(r"\b(or|and)\s+\w+\s+(acid|base|ether|solvent|catalyst)\b", re.I),
    re.compile(r"\b(with|via|from|using|after)\s+\w", re.I),
    re.compile(r"(conc\.|dry|aq\.)\s+\w", re.I),
    re.compile(r"\b(allyl ester|allyl bromide|allyl ether)\b", re.I),  # 너무 일반적
    re.compile(r"\b(aldehyde or|ketone or|acid or)\b", re.I),
    re.compile(r"\bCX2\b"),                                # "pyrrole, CX2" 류
    re.compile(r"\bTS\*?\b"),                              # transition state
]

# 순수 SMILES 패턴 — 이것들은 PubChem 불필요, RDKit 직접 파싱
# 소문자가 링 원자(방향족)이고 [] 없이 숫자만 있는 SMILES 패턴
PURE_SMILES_RE = re.compile(
    r"^[A-Za-z@\[\]()\-=#$\\/\.+%0-9:]{4,}$"  # 공백 없는 SMILES 후보
)


def classify_text(text: str) -> str:
    """
    'smiles'  — RDKit 직접 파싱
    'name'    — PubChem 이름 조회
    'skip'    — 제외
    """
    if not text:
        return "skip"
    t = text.strip()
    if len(t) < 5 or len(t) > 80:
        return "skip"
    if t in SKIP_EXACT:
        return "skip"

    # 공백 없고 SMILES 문자만 있으면 RDKit 직접 시도
    if " " not in t and PURE_SMILES_RE.match(t):
        mol = Chem.MolFromSmiles(t)
        if mol is not None:
            return "smiles"

    # 소문자 영문 3자 이상 없으면 약어/원소기호
    if not re.search(r"[a-z]{3,}", t):
        return "skip"
    # 쉼표 2개 이상 = 복합 혼합물
    if t.count(",") > 1:
        return "skip"
    for pat in SKIP_RE:
        if pat.search(t):
            return "skip"

    return "name"


# ── RDKit 유틸 ────────────────────────────────────────────────────────────────
def canonicalize(smiles: str) -> Optional[str]:
    try:
        mol = Chem.MolFromSmiles(smiles)
        return Chem.MolToSmiles(mol, canonical=True) if mol else None
    except Exception:
        return None


def morgan_fp_bytes(smiles: str) -> Optional[bytes]:
    try:
        from rdkit.Chem import AllChem
        from rdkit import DataStructs
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            return None
        fp = AllChem.GetMorganFingerprintAsBitVect(mol, radius=2, nBits=2048)
        return DataStructs.BitVectToText(fp).encode()
    except Exception:
        return None


# ── PubChem ───────────────────────────────────────────────────────────────────
def pubchem_lookup(name: str, delay: float, timeout: int = 10) -> Optional[str]:
    """429/503 시 최대 3회 exponential backoff."""
    url = PUBCHEM_URL.format(name=requests.utils.quote(name.strip()))
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200:
                props = r.json().get("PropertyTable", {}).get("Properties", [])
                if props:
                    p = props[0]
                    return (p.get("IsomericSMILES") or
                            p.get("CanonicalSMILES") or
                            p.get("SMILES"))
                return None
            if r.status_code == 404:
                return None
            if r.status_code in (429, 503):
                wait = delay * (3 ** attempt) * 2
                log.debug(f"HTTP {r.status_code} — {wait:.1f}s 후 재시도 ({attempt+1}/3)")
                time.sleep(wait)
                continue
            log.debug(f"HTTP {r.status_code} for {name!r:.40}")
            return None
        except Exception as e:
            log.debug(f"예외 {name!r:.40}: {e}")
            if attempt < 2:
                time.sleep(delay * 2)
    return None


# ── 통계 ─────────────────────────────────────────────────────────────────────
def coverage_stats(cur: sqlite3.Cursor) -> dict:
    cur.execute("SELECT count(*) FROM extract_molecules WHERE queryable=1")
    q = cur.fetchone()[0]
    cur.execute("SELECT count(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1")
    f = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM extract_molecules WHERE quality_tier=1 AND queryable=1")
    t1 = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM extract_molecules WHERE quality_tier=3")
    t3 = cur.fetchone()[0]
    return {"queryable": q, "families": f, "tier1": t1, "tier3": t3}


# ── 메인 ─────────────────────────────────────────────────────────────────────
def run(db_path: Path, dry_run: bool, delay: float) -> None:
    if not db_path.exists():
        log.error(f"DB 없음: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    before = coverage_stats(cur)
    log.info("=== 실행 전 ===")
    log.info(f"  queryable=1:     {before['queryable']}")
    log.info(f"  tier1:           {before['tier1']}")
    log.info(f"  tier3:           {before['tier3']}")
    log.info(f"  구조검색 family: {before['families']}")

    cur.execute("""
        SELECT id, extract_id, role, normalized_text, reaction_family_name,
               source_zip, page_no, source_field, note_text
        FROM extract_molecules
        WHERE quality_tier=3 AND smiles IS NULL AND normalized_text IS NOT NULL
        ORDER BY id
    """)
    rows = cur.fetchall()

    smiles_rows, name_rows, skip_rows = [], [], []
    for r in rows:
        cls = classify_text(r["normalized_text"])
        if cls == "smiles":
            smiles_rows.append(r)
        elif cls == "name":
            name_rows.append(r)
        else:
            skip_rows.append(r)

    unique_names = list(dict.fromkeys(r["normalized_text"].strip() for r in name_rows))

    log.info(f"\ntier3 전체:          {len(rows)}")
    log.info(f"  SMILES 직접 파싱:  {len(smiles_rows)}")
    log.info(f"  PubChem 이름 조회: {len(name_rows)}  (unique: {len(unique_names)})")
    log.info(f"  제외:              {len(skip_rows)}")
    log.info(f"예상 소요 시간:      {len(unique_names)*delay/60:.1f}분")

    # ── PubChem 조회 ──────────────────────────────────────────────────────
    cache: dict[str, Optional[str]] = {}
    api_ok = no_hit = http_fail = 0

    if unique_names:
        log.info(f"\nPubChem 조회 시작... ({len(unique_names)}개)")
        for i, name in enumerate(unique_names):
            if i > 0 and i % 20 == 0:
                pct = i / len(unique_names) * 100
                log.info(f"  진행 {i}/{len(unique_names)} ({pct:.0f}%)  성공:{api_ok} no_hit:{no_hit} http_fail:{http_fail}")
            raw = pubchem_lookup(name, delay=delay)
            if raw is None:
                # pubchem_lookup이 None을 반환한 경우 — HTTP 실패와 no-hit 구분 불가
                # 여기서는 no_hit로 집계 (HTTP 실패는 pubchem_lookup 내부 log.debug)
                no_hit += 1
                cache[name] = None
                log.debug(f"  --- {name!r:.50}")
            else:
                canon = canonicalize(raw)
                cache[name] = canon
                if canon:
                    api_ok += 1
                    log.info(f"  OK  {name!r:.50} → {canon[:50]}")
                else:
                    no_hit += 1
                    log.debug(f"  RDKit fail {name!r:.50}")
            time.sleep(delay)
        log.info(f"조회 완료: 성공={api_ok}  no_hit={no_hit}  http_fail={http_fail}  "
                 f"성공률={api_ok/len(unique_names)*100:.0f}%")

    # ── 중복 체크 ────────────────────────────────────────────────────────
    cur.execute("SELECT extract_id, role, smiles FROM extract_molecules WHERE smiles IS NOT NULL")
    existing = set((r[0], r[1], r[2]) for r in cur.fetchall())

    cur.execute("SELECT DISTINCT reaction_family_name FROM extract_molecules WHERE queryable=1")
    existing_families = set(r[0] for r in cur.fetchall())

    # ── UPDATE 공통 함수 ─────────────────────────────────────────────────
    updated = skipped_dup = 0
    projected_new_families: set[str] = set()

    def do_update(row, canon: str, src: str, conf: float) -> None:
        nonlocal updated, skipped_dup
        key = (row["extract_id"], row["role"], canon)
        if key in existing:
            skipped_dup += 1
            return
        orig = row["normalized_text"].strip()
        note_old = row["note_text"] or ""
        new_note = (f"[{src}: {orig}]"
                    if not note_old else f"[{src}: {orig}] {note_old}")
        fp = morgan_fp_bytes(canon)
        if not dry_run:
            cur.execute("""
                UPDATE extract_molecules
                SET smiles=?, smiles_kind='exact', quality_tier=1, queryable=1,
                    morgan_fp=?, normalized_text=?, structure_source=?,
                    note_text=?, role_confidence=?
                WHERE id=?
            """, (canon, fp, canon, src, new_note, conf, row["id"]))
        existing.add(key)
        updated += 1
        fam = row["reaction_family_name"]
        if fam and fam not in existing_families:
            projected_new_families.add(fam)

    # RDKit 직접
    rdkit_ok = rdkit_fail = 0
    for row in smiles_rows:
        canon = canonicalize(row["normalized_text"].strip())
        if canon:
            do_update(row, canon, STRUCTURE_SOURCE_RDKIT, 0.88)
            rdkit_ok += 1
        else:
            rdkit_fail += 1
    log.info(f"\nRDKit 직접 파싱: 성공={rdkit_ok} 실패={rdkit_fail}")

    # PubChem
    skipped_no_smiles = 0
    for row in name_rows:
        canon = cache.get(row["normalized_text"].strip())
        if not canon:
            skipped_no_smiles += 1
            continue
        do_update(row, canon, STRUCTURE_SOURCE_PUBCHEM, 0.75)

    if not dry_run:
        conn.commit()

    after = coverage_stats(cur)
    conn.close()

    # ── 최종 출력 ─────────────────────────────────────────────────────────
    log.info("")
    log.info("=== 결과 ===")
    if dry_run:
        log.info(f"  [DRY-RUN] DB 변경 없음")
        log.info(f"  예상 UPDATE (RDKit):    {rdkit_ok}개")
        log.info(f"  예상 UPDATE (PubChem):  {updated - rdkit_ok}개")
        log.info(f"  예상 UPDATE 합계:       {updated}개")
        log.info(f"  skip (SMILES 없음):     {skipped_no_smiles + rdkit_fail}개")
        log.info(f"  skip (중복):            {skipped_dup}개")
        log.info("")
        log.info("=== 예상 전/후 비교 (projected) ===")
        log.info(f"  queryable=1:  {before['queryable']} → {before['queryable']+updated}  (Δ+{updated})")
        log.info(f"  tier1:        {before['tier1']} → {before['tier1']+updated}  (Δ+{updated})")
        log.info(f"  tier3:        {before['tier3']} → {before['tier3']-updated}  (Δ-{updated})")
        log.info(f"  family 커버:  {before['families']} → "
                 f"{before['families']+len(projected_new_families)}  "
                 f"(Δ+{len(projected_new_families)})")
        if projected_new_families:
            log.info(f"  신규 진입 family ({len(projected_new_families)}개):")
            for f in sorted(projected_new_families):
                log.info(f"    {f}")
    else:
        log.info(f"  UPDATE (RDKit):    {rdkit_ok}개")
        log.info(f"  UPDATE (PubChem):  {updated - rdkit_ok}개")
        log.info(f"  UPDATE 합계:       {updated}개")
        log.info(f"  skip (SMILES 없음): {skipped_no_smiles + rdkit_fail}개")
        log.info(f"  skip (중복):       {skipped_dup}개")
        log.info("")
        log.info("=== 실제 전/후 비교 ===")
        log.info(f"  queryable=1:  {before['queryable']} → {after['queryable']}  (Δ{after['queryable']-before['queryable']})")
        log.info(f"  tier1:        {before['tier1']} → {after['tier1']}  (Δ{after['tier1']-before['tier1']})")
        log.info(f"  tier3:        {before['tier3']} → {after['tier3']}  (Δ{before['tier3']-after['tier3']} 감소)")
        log.info(f"  family 커버:  {before['families']} → {after['families']}  (Δ{after['families']-before['families']})")
        if projected_new_families:
            log.info(f"  신규 진입 family ({len(projected_new_families)}개):")
            for f in sorted(projected_new_families):
                log.info(f"    {f}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="tier3 → PubChem / RDKit direct → tier1 승격 (v2 final)"
    )
    parser.add_argument("--db", default="app/labint.db")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--delay", type=float, default=0.35,
                        help="PubChem API 간격(초, default=0.35)")
    args = parser.parse_args()
    log.info(f"DB: {Path(args.db).resolve()}")
    log.info(f"dry-run: {args.dry_run}  delay: {args.delay}s")
    run(Path(args.db), dry_run=args.dry_run, delay=args.delay)


if __name__ == "__main__":
    main()
