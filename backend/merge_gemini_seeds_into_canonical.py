"""
merge_gemini_seeds_into_canonical.py
=====================================

목적
----
실험본 DB에 쌓인 gemini_auto_seed / deterministic_gemini_seed 데이터를
canonical DB(labint.db)에 안전하게 병합한다.

원칙
----
1. 반드시 dry-run 먼저. --apply 플래그가 있을 때만 실제 변경.
2. 실행 전 자동 백업 (타임스탬프 붙은 파일명)
3. SQL transaction으로 묶어 처리. 중간 오류 시 전체 rollback.
4. 병합 전후 수치를 stdout에 찍어 검증 가능.
5. id 충돌 전수 검사. 충돌 있으면 즉시 중단.
6. append-only 병합. 기존 canonical 레코드는 절대 수정하지 않음.

병합 대상
--------
- reaction_extracts: structure_source in ('gemini_auto_seed', 'deterministic_gemini_seed', 'deterministic_seed_from_existing') 분자가 연결된 extract들
- extract_molecules: structure_source in ('gemini_auto_seed', 'deterministic_gemini_seed', 'deterministic_seed_from_existing') 인 모든 분자
- reaction_family_patterns: experimental과 canonical의 diff (없으면 skip)

실행
----
# dry-run (DB 변경 없이 예상치만 출력)
python merge_gemini_seeds_into_canonical.py --dry-run

# 실제 병합
python merge_gemini_seeds_into_canonical.py --apply

# 경로 지정
python merge_gemini_seeds_into_canonical.py --apply \
    --canonical app/labint.db \
    --source app/labint_gemini_autorun_continuous.db
"""
from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------
# 유틸
# ---------------------------------------------------------------

def get_state(conn: sqlite3.Connection) -> dict:
    """DB의 주요 지표를 한 번에 읽는다."""
    cur = conn.cursor()
    return {
        "queryable": cur.execute(
            "SELECT COUNT(*) FROM extract_molecules WHERE queryable=1"
        ).fetchone()[0],
        "tier1": cur.execute(
            "SELECT COUNT(*) FROM extract_molecules WHERE queryable=1 AND quality_tier=1"
        ).fetchone()[0],
        "tier2": cur.execute(
            "SELECT COUNT(*) FROM extract_molecules WHERE queryable=1 AND quality_tier=2"
        ).fetchone()[0],
        "family_coverage": cur.execute(
            "SELECT COUNT(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1"
        ).fetchone()[0],
        "reaction_extracts": cur.execute(
            "SELECT COUNT(*) FROM reaction_extracts"
        ).fetchone()[0],
        "extract_molecules_total": cur.execute(
            "SELECT COUNT(*) FROM extract_molecules"
        ).fetchone()[0],
        "reaction_family_patterns": cur.execute(
            "SELECT COUNT(*) FROM reaction_family_patterns"
        ).fetchone()[0],
        "curated_seed_total": cur.execute(
            "SELECT COUNT(*) FROM extract_molecules WHERE structure_source IN ('gemini_auto_seed','deterministic_gemini_seed','deterministic_seed_from_existing')"
        ).fetchone()[0],
        "deterministic_gemini_seed": cur.execute(
            "SELECT COUNT(*) FROM extract_molecules WHERE structure_source='deterministic_gemini_seed'"
        ).fetchone()[0],
    }


def print_state(label: str, state: dict) -> None:
    print(f"  [{label}]")
    for k, v in state.items():
        print(f"    {k:30s} = {v}")


def print_diff(before: dict, after: dict) -> None:
    print("  [diff (after - before)]")
    for k in before:
        delta = after[k] - before[k]
        sign = "+" if delta > 0 else ("" if delta == 0 else "")
        marker = "  " if delta == 0 else "→ "
        print(f"  {marker}{k:30s} = {before[k]:6d} → {after[k]:6d} ({sign}{delta})")


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def get_new_ids(canonical: sqlite3.Connection, source: sqlite3.Connection,
                table: str) -> list[int]:
    """source에만 있는 id 목록."""
    c_ids = {r[0] for r in canonical.execute(f"SELECT id FROM {table}")}
    s_ids = {r[0] for r in source.execute(f"SELECT id FROM {table}")}
    return sorted(s_ids - c_ids)


def check_id_conflicts(canonical: sqlite3.Connection, table: str,
                       new_ids: list[int]) -> list[int]:
    """new_ids 중 canonical에 이미 존재하는 id를 반환. (없어야 정상)"""
    conflicts = []
    for i in range(0, len(new_ids), 500):
        batch = new_ids[i:i + 500]
        placeholders = ",".join("?" * len(batch))
        rows = canonical.execute(
            f"SELECT id FROM {table} WHERE id IN ({placeholders})", batch
        ).fetchall()
        conflicts.extend(r[0] for r in rows)
    return conflicts


def copy_rows(canonical: sqlite3.Connection, source: sqlite3.Connection,
              table: str, ids: list[int]) -> int:
    """source의 특정 id들을 canonical에 그대로 복사."""
    if not ids:
        return 0
    cols = table_columns(canonical, table)
    source_cols = table_columns(source, table)
    # source에는 있지만 canonical에는 없는 컬럼은 제외 (append-only이므로)
    common_cols = [c for c in cols if c in source_cols]
    col_list = ",".join(common_cols)
    placeholders = ",".join("?" * len(common_cols))
    inserted = 0
    for i in range(0, len(ids), 500):
        batch = ids[i:i + 500]
        in_clause = ",".join("?" * len(batch))
        rows = source.execute(
            f"SELECT {col_list} FROM {table} WHERE id IN ({in_clause})", batch
        ).fetchall()
        canonical.executemany(
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})",
            rows,
        )
        inserted += len(rows)
    return inserted


# ---------------------------------------------------------------
# 메인
# ---------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--canonical", default="app/labint.db",
                        help="목적지 DB (기본: app/labint.db)")
    parser.add_argument("--source", default="app/labint_gemini_autorun_continuous.db",
                        help="병합 소스 DB (continuous/v4 stage DB)")
    parser.add_argument("--dry-run", action="store_true",
                        help="실제 변경 없이 예상 수치만 출력 (기본)")
    parser.add_argument("--apply", action="store_true",
                        help="실제로 DB 병합. 지정 안 하면 dry-run.")
    parser.add_argument("--no-backup", action="store_true",
                        help="백업 생성 생략 (비권장). --apply에서만 유효.")
    parser.add_argument("--source-kinds", default="gemini_auto_seed,deterministic_gemini_seed,deterministic_seed_from_existing",
                        help="쉼표로 구분된 structure_source 목록을 병합 대상으로 사용")
    args = parser.parse_args()

    # 경로 검증
    canonical_path = Path(args.canonical).resolve()
    source_path = Path(args.source).resolve()

    if not canonical_path.exists():
        print(f"[ERROR] canonical DB not found: {canonical_path}")
        return 1
    if not source_path.exists():
        print(f"[ERROR] source DB not found: {source_path}")
        return 1
    if canonical_path == source_path:
        print("[ERROR] canonical과 source가 같은 파일입니다. 중단.")
        return 1

    is_dry_run = not args.apply
    source_kinds = [s.strip() for s in args.source_kinds.split(",") if s.strip()]

    print("=" * 70)
    print("MERGE Gemini seeds INTO canonical labint.db")
    print("=" * 70)
    print(f"  canonical: {canonical_path}")
    print(f"  source:    {source_path}")
    print(f"  mode:      {'DRY-RUN' if is_dry_run else 'APPLY (실제 병합)'}")
    print()

    # 1. 백업
    backup_path = None
    if args.apply and not args.no_backup:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = canonical_path.parent / f"{canonical_path.stem}.backup_before_gemini_merge_{ts}.db"
        print(f"[STEP 1] 백업 생성: {backup_path.name}")
        shutil.copy2(canonical_path, backup_path)
        print(f"  OK ({backup_path.stat().st_size:,} bytes)")
        print()
    elif args.apply and args.no_backup:
        print("[STEP 1] 백업 생략 (--no-backup)")
        print()
    else:
        print("[STEP 1] (dry-run: 백업 생략)")
        print()

    # 2. 병합 대상 식별
    canonical = sqlite3.connect(str(canonical_path))
    source = sqlite3.connect(str(source_path))
    try:
        print("[STEP 2] 병합 대상 식별")

        # extract_molecules 신규 — Gemini seed 계열만 타겟
        seed_sources = ("gemini_auto_seed", "deterministic_gemini_seed")
        placeholders = ",".join("?" * len(seed_sources))
        em_source_ids = [
            r[0] for r in source.execute(
                f"SELECT id FROM extract_molecules WHERE structure_source IN ({placeholders}) ORDER BY id",
                seed_sources,
            )
        ]
        em_canonical_ids = {r[0] for r in canonical.execute("SELECT id FROM extract_molecules")}
        new_em_ids = [mid for mid in em_source_ids if mid not in em_canonical_ids]
        print(f"  extract_molecules 신규 (gemini_auto_seed + deterministic_gemini_seed): {len(new_em_ids)}개")

        # reaction_extracts 신규 — 새 seed molecules가 참조하는 extract만 대상
        if new_em_ids:
            placeholders_em = ",".join("?" * len(new_em_ids))
            referenced_re_ids = {
                r[0] for r in source.execute(
                    f"SELECT DISTINCT extract_id FROM extract_molecules WHERE id IN ({placeholders_em})",
                    new_em_ids,
                )
            }
        else:
            referenced_re_ids = set()
        canonical_re_ids = {r[0] for r in canonical.execute("SELECT id FROM reaction_extracts")}
        new_re_ids = sorted(referenced_re_ids - canonical_re_ids)
        print(f"  reaction_extracts 신규 (seed-linked): {len(new_re_ids)}개 (id={new_re_ids[:3]}...{new_re_ids[-3:] if len(new_re_ids)>3 else ''})")

        # reaction_family_patterns 신규
        new_fp_ids = get_new_ids(canonical, source, "reaction_family_patterns")
        print(f"  reaction_family_patterns 신규: {len(new_fp_ids)}개")

        print()

        # 3. id 충돌 전수 검사
        print("[STEP 3] id 충돌 검사 (canonical에 이미 존재하는 id)")
        conflicts_re = check_id_conflicts(canonical, "reaction_extracts", new_re_ids)
        conflicts_em = check_id_conflicts(canonical, "extract_molecules", new_em_ids)
        conflicts_fp = check_id_conflicts(canonical, "reaction_family_patterns", new_fp_ids)

        print(f"  reaction_extracts 충돌: {len(conflicts_re)}")
        print(f"  extract_molecules 충돌: {len(conflicts_em)}")
        print(f"  reaction_family_patterns 충돌: {len(conflicts_fp)}")

        if conflicts_re or conflicts_em or conflicts_fp:
            print("[ERROR] id 충돌이 발견됐습니다. 병합 중단.")
            print("  이 상태는 append-only 병합이 불가능함을 의미합니다.")
            return 2
        print("  OK (충돌 없음)")
        print()

        # 4. FK 일관성 검증
        print("[STEP 4] FK 일관성 검증")
        # extract_molecules.extract_id가 reaction_extracts.id를 가리키는지
        # 신규 em이 신규 re 또는 기존 re 중 하나를 가리켜야 함
        em_fk = set()
        if new_em_ids:
            placeholders = ",".join("?" * len(new_em_ids))
            for r in source.execute(
                f"SELECT DISTINCT extract_id FROM extract_molecules WHERE id IN ({placeholders})",
                new_em_ids
            ):
                em_fk.add(r[0])

        all_re_after_merge = {r[0] for r in canonical.execute("SELECT id FROM reaction_extracts")}
        all_re_after_merge.update(new_re_ids)

        orphans = em_fk - all_re_after_merge
        print(f"  신규 extract_molecules가 가리키는 extract_id: {len(em_fk)}개")
        print(f"  그중 고아 (FK 끊김): {len(orphans)}")
        if orphans:
            print(f"  [ERROR] 고아 FK 발견: {sorted(orphans)[:10]}")
            print(f"  이 분자들을 병합하면 DB 일관성이 깨집니다. 중단.")
            return 3
        print("  OK")
        print()

        # 5. 병합 전 상태
        print("[STEP 5] 병합 전 canonical 상태")
        before = get_state(canonical)
        print_state("BEFORE", before)
        print()

        # 6. 실제 병합 (dry-run이면 skip)
        if is_dry_run:
            # dry-run 예상치 계산
            print("[STEP 6] (DRY-RUN) 실제 병합 생략")
            print("  예상 변경:")
            print(f"    + reaction_extracts           {len(new_re_ids)}")
            print(f"    + extract_molecules           {len(new_em_ids)}")
            print(f"    + reaction_family_patterns    {len(new_fp_ids)}")
            print()

            # 예상 after state (source의 값들을 projection)
            # 새 em 중 queryable=1인 것들
            if new_em_ids:
                placeholders = ",".join("?" * len(new_em_ids))
                proj = source.execute(
                    f"""SELECT 
                        SUM(CASE WHEN queryable=1 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN queryable=1 AND quality_tier=1 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN queryable=1 AND quality_tier=2 THEN 1 ELSE 0 END),
                        COUNT(DISTINCT CASE WHEN queryable=1 THEN reaction_family_name END)
                       FROM extract_molecules WHERE id IN ({placeholders})""",
                    new_em_ids
                ).fetchone()
                new_queryable_delta, new_t1_delta, new_t2_delta, new_fam_hit = proj

                # canonical에 없는 새 family들
                c_fams = {r[0] for r in canonical.execute(
                    "SELECT DISTINCT reaction_family_name FROM extract_molecules WHERE queryable=1"
                )}
                s_new_fams_rows = source.execute(
                    f"""SELECT DISTINCT reaction_family_name FROM extract_molecules 
                       WHERE queryable=1 AND id IN ({placeholders})""",
                    new_em_ids
                ).fetchall()
                s_new_fams = {r[0] for r in s_new_fams_rows}
                really_new_fams = s_new_fams - c_fams
                fam_cov_delta = len(really_new_fams)

                projected = {
                    "queryable": before["queryable"] + (new_queryable_delta or 0),
                    "tier1": before["tier1"] + (new_t1_delta or 0),
                    "tier2": before["tier2"] + (new_t2_delta or 0),
                    "family_coverage": before["family_coverage"] + fam_cov_delta,
                    "reaction_extracts": before["reaction_extracts"] + len(new_re_ids),
                    "extract_molecules_total": before["extract_molecules_total"] + len(new_em_ids),
                    "reaction_family_patterns": before["reaction_family_patterns"] + len(new_fp_ids),
                    "gemini_auto_seed": before["gemini_auto_seed"] + len(new_em_ids),
                }
                print("  예상 병합 후 상태:")
                print_state("PROJECTED AFTER", projected)
                print()
                print_diff(before, projected)
                print()
                print("  새로 queryable 가능해지는 family들:")
                for f in sorted(really_new_fams):
                    print(f"    + {f}")

            print()
            print("=" * 70)
            print("[완료] DRY-RUN. 실제로 병합하려면 --apply 추가")
            print("=" * 70)
            return 0

        # 실제 병합
        print("[STEP 6] 실제 병합 진행")
        canonical.execute("BEGIN")
        try:
            # 순서 중요: reaction_family_patterns → reaction_extracts → extract_molecules
            # (FK: em.extract_id -> re.id)
            n_fp = copy_rows(canonical, source, "reaction_family_patterns", new_fp_ids)
            print(f"  reaction_family_patterns: +{n_fp}")

            n_re = copy_rows(canonical, source, "reaction_extracts", new_re_ids)
            print(f"  reaction_extracts: +{n_re}")

            n_em = copy_rows(canonical, source, "extract_molecules", new_em_ids)
            print(f"  extract_molecules: +{n_em}")

            canonical.commit()
            print("  COMMIT OK")
        except Exception as e:
            canonical.rollback()
            print(f"[ERROR] 병합 중 오류, rollback: {e}")
            return 4
        print()

        # 7. 병합 후 상태
        print("[STEP 7] 병합 후 canonical 상태")
        after = get_state(canonical)
        print_state("AFTER", after)
        print()
        print_diff(before, after)
        print()

        # 8. 검증
        print("[STEP 8] 사후 검증")
        expected_em_delta = len(new_em_ids)
        actual_em_delta = after["extract_molecules_total"] - before["extract_molecules_total"]
        if actual_em_delta != expected_em_delta:
            print(f"[WARNING] extract_molecules 수 불일치: expected={expected_em_delta}, actual={actual_em_delta}")
        else:
            print(f"  extract_molecules delta: {actual_em_delta} OK")

        # FK integrity
        orphan_count = canonical.execute("""
            SELECT COUNT(*) FROM extract_molecules em
            LEFT JOIN reaction_extracts re ON em.extract_id = re.id
            WHERE re.id IS NULL
        """).fetchone()[0]
        if orphan_count > 0:
            print(f"[WARNING] 고아 extract_molecules: {orphan_count}")
        else:
            print("  FK integrity: OK (고아 없음)")

        print()
        print("=" * 70)
        print(f"[완료] 병합 성공")
        if backup_path:
            print(f"  백업: {backup_path}")
            print(f"  (문제 발생 시: copy \"{backup_path.name}\" \"{canonical_path.name}\")")
        print("=" * 70)
        return 0

    finally:
        canonical.close()
        source.close()


if __name__ == "__main__":
    sys.exit(main())
