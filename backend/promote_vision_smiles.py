"""
promote_vision_smiles.py
========================
scheme_candidates.vision_raw_json 안에 이미 채워진 reactant_smiles / product_smiles를
extract_molecules 테이블에 직접 tier1 (queryable=1) 레코드로 promote한다.

실행:
    cd backend/
    python promote_vision_smiles.py [--db app/labint.db] [--dry-run]

설계 원칙:
    1. RDKit canonicalization  — 원본 SMILES를 그대로 넣지 않고 정규화
    2. 중복 방지              — (extract_id, role, normalized_smiles) 기준 skip
    3. provenance 표시        — structure_source = 'vision_raw_json_promote'
    4. 애매한 매핑 로그       — sc_id+family 연결이 복수일 때 WARNING 출력
    5. 전/후 통계 출력        — insert/skip/family 커버리지 변화
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s  %(message)s",
)
log = logging.getLogger("promote_vision")

# ── RDKit (없으면 종료) ───────────────────────────────────────────────────────
try:
    from rdkit import Chem
    from rdkit import RDLogger
    RDLogger.DisableLog("rdApp.*")
except ImportError:
    log.error("RDKit이 설치되지 않았습니다. pip install rdkit 후 재실행하세요.")
    sys.exit(1)


# ── 상수 ─────────────────────────────────────────────────────────────────────
STRUCTURE_SOURCE = "vision_raw_json_promote"
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── 헬퍼 ─────────────────────────────────────────────────────────────────────
def canonicalize(smiles: Optional[str]) -> Optional[str]:
    """RDKit으로 canonical SMILES 반환. 파싱 실패 시 None."""
    if not smiles:
        return None
    try:
        mol = Chem.MolFromSmiles(smiles.strip())
        if mol is None:
            return None
        return Chem.MolToSmiles(mol, canonical=True)
    except Exception:
        return None


def morgan_fp_bytes(smiles: str) -> Optional[bytes]:
    """Morgan fingerprint (radius=2, 2048bit) → bytes. 실패 시 None."""
    try:
        from rdkit.Chem import AllChem
        from rdkit import DataStructs
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            return None
        fp = AllChem.GetMorganFingerprintAsBitVect(mol, radius=2, nBits=2048)
        arr = DataStructs.BitVectToText(fp).encode()
        return arr
    except Exception:
        return None


# ── 전/후 통계 ────────────────────────────────────────────────────────────────
def coverage_stats(cur: sqlite3.Cursor) -> dict:
    cur.execute("SELECT count(*) FROM extract_molecules WHERE queryable=1")
    queryable = cur.fetchone()[0]
    cur.execute(
        "SELECT count(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1"
    )
    families = cur.fetchone()[0]
    cur.execute(
        "SELECT count(*) FROM extract_molecules WHERE quality_tier=1 AND queryable=1"
    )
    tier1 = cur.fetchone()[0]
    return {"queryable": queryable, "families": families, "tier1": tier1}


# ── 핵심 로직 ─────────────────────────────────────────────────────────────────
def collect_promote_items(cur: sqlite3.Cursor) -> list[dict]:
    """
    scheme_candidates.vision_raw_json 에서 SMILES가 채워진 extract를 수집.
    reaction_extracts.id(extract_id)는 sc_id + family 기준 첫 번째 행으로 연결.
    """
    cur.execute(
        """
        SELECT sc.id AS sc_id,
               sc.vision_raw_json,
               pi.source_zip,
               pi.page_no
        FROM scheme_candidates sc
        JOIN page_images pi ON sc.page_image_id = pi.id
        WHERE sc.vision_raw_json IS NOT NULL
        ORDER BY sc.id
        """
    )
    sc_rows = cur.fetchall()

    items = []
    ambiguous_log = []

    for sc_id, raw_json, source_zip, page_no in sc_rows:
        try:
            page_data = json.loads(raw_json)
        except Exception:
            log.warning(f"sc_id={sc_id}: vision_raw_json JSON 파싱 실패, skip")
            continue

        for sc_item in page_data.get("scheme_candidates", []):
            for extract in sc_item.get("extracts", []):
                r_smi_raw = extract.get("reactant_smiles")
                p_smi_raw = extract.get("product_smiles")
                conf = float(extract.get("smiles_confidence") or 0.0)

                # SMILES가 없거나 confidence 0이면 skip
                if not (r_smi_raw or p_smi_raw) or conf <= 0.0:
                    continue

                family = extract.get("reaction_family_name") or page_data.get(
                    "reaction_family_name"
                )
                if not family:
                    log.warning(f"sc_id={sc_id}: family 없음, skip")
                    continue

                # reaction_extracts.id 찾기 (sc_id + family 기준)
                cur.execute(
                    """
                    SELECT id FROM reaction_extracts
                    WHERE scheme_candidate_id = ? AND reaction_family_name = ?
                    ORDER BY id
                    """,
                    (sc_id, family),
                )
                re_rows = cur.fetchall()

                if not re_rows:
                    log.warning(
                        f"sc_id={sc_id} [{family}]: reaction_extracts 매칭 없음, skip"
                    )
                    continue

                extract_id = re_rows[0][0]  # 첫 번째 사용

                if len(re_rows) > 1:
                    # 복수 매핑 → 로그만 남기고 진행
                    ambiguous_log.append(
                        f"sc_id={sc_id} [{family}]: {len(re_rows)}개 reaction_extracts 매칭 "
                        f"→ extract_id={extract_id} 사용"
                    )

                # reactant / product 각각 처리
                for role, raw_smi in [("reactant", r_smi_raw), ("product", p_smi_raw)]:
                    if not raw_smi:
                        continue
                    # 점 구분 multi-component SMILES도 canonical로 통과
                    canon = canonicalize(raw_smi)
                    if canon is None:
                        log.debug(
                            f"sc_id={sc_id} [{family}] {role}: "
                            f"SMILES 파싱 실패 {raw_smi!r:.40}, skip"
                        )
                        continue

                    items.append(
                        {
                            "extract_id": extract_id,
                            "role": role,
                            "smiles": canon,
                            "smiles_raw": raw_smi,
                            "smiles_confidence": conf,
                            "reaction_family_name": family,
                            "source_zip": source_zip,
                            "page_no": page_no,
                        }
                    )

    if ambiguous_log:
        log.warning(f"복수 매핑 케이스 {len(ambiguous_log)}건:")
        for msg in ambiguous_log[:10]:
            log.warning(f"  {msg}")
        if len(ambiguous_log) > 10:
            log.warning(f"  ... 외 {len(ambiguous_log)-10}건 (생략)")

    return items


def run(db_path: Path, dry_run: bool) -> None:
    if not db_path.exists():
        log.error(f"DB를 찾을 수 없습니다: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # ── 전 통계
    before = coverage_stats(cur)
    log.info("=== 실행 전 ===")
    log.info(f"  queryable=1 총:   {before['queryable']}")
    log.info(f"  tier1 (queryable): {before['tier1']}")
    log.info(f"  구조검색 family:   {before['families']}")

    # ── 수집
    log.info("vision_raw_json에서 SMILES 수집 중...")
    items = collect_promote_items(cur)
    log.info(f"수집된 후보: {len(items)}개 (reactant+product 합산)")

    # ── 중복 체크용 기존 (extract_id, role, smiles) 셋
    cur.execute(
        "SELECT extract_id, role, smiles FROM extract_molecules WHERE smiles IS NOT NULL"
    )
    existing = set((r[0], r[1], r[2]) for r in cur.fetchall())
    log.info(f"기존 extract_molecules SMILES 레코드: {len(existing)}개")

    # ── INSERT
    inserted = 0
    skipped_dup = 0
    skipped_rdkit = 0

    for item in items:
        key = (item["extract_id"], item["role"], item["smiles"])
        if key in existing:
            skipped_dup += 1
            continue

        fp = morgan_fp_bytes(item["smiles"])

        if not dry_run:
            cur.execute(
                """
                INSERT INTO extract_molecules
                    (extract_id, role, smiles, smiles_kind, quality_tier,
                     reaction_family_name, source_zip, page_no,
                     queryable, note_text, morgan_fp,
                     normalized_text, source_field, structure_source,
                     alias_id, fg_tags, role_confidence, created_at)
                VALUES
                    (?, ?, ?, 'exact', 1,
                     ?, ?, ?,
                     1, ?, ?,
                     ?, 'vision_raw_json', ?,
                     NULL, NULL, ?, ?)
                """,
                (
                    item["extract_id"],
                    item["role"],
                    item["smiles"],
                    item["reaction_family_name"],
                    item["source_zip"],
                    item["page_no"],
                    item["smiles_raw"],          # note_text = 원본
                    fp,
                    item["smiles"],              # normalized_text = canon
                    STRUCTURE_SOURCE,
                    item["smiles_confidence"],
                    NOW,
                ),
            )
        existing.add(key)
        inserted += 1

    if not dry_run:
        conn.commit()

    # ── 후 통계
    after = coverage_stats(cur)
    conn.close()

    log.info("")
    log.info("=== 결과 ===")
    log.info(f"  INSERT:        {inserted}개{'  (dry-run: 실제 반영 안 됨)' if dry_run else ''}")
    log.info(f"  skip (중복):   {skipped_dup}개")
    log.info(f"  skip (RDKit):  {skipped_rdkit}개")
    log.info("")
    log.info("=== 전/후 비교 ===")
    log.info(f"  queryable=1:   {before['queryable']} → {after['queryable']}  (Δ{after['queryable']-before['queryable']})")
    log.info(f"  tier1:         {before['tier1']} → {after['tier1']}  (Δ{after['tier1']-before['tier1']})")
    log.info(f"  family 커버:   {before['families']} → {after['families']}  (Δ{after['families']-before['families']})")


# ── CLI ───────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="vision_raw_json SMILES → extract_molecules promote")
    parser.add_argument(
        "--db",
        default="app/labint.db",
        help="DB 경로 (default: app/labint.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="실제 INSERT 없이 통계만 출력",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    log.info(f"DB: {db_path.resolve()}")
    log.info(f"dry-run: {args.dry_run}")
    run(db_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
