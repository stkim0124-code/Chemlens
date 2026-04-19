CHEMLENS - Rejected 8 Family Injection Patch
============================================
작성일: 2026-04-18

## 이 패치가 하는 것

rejected된 6개(+Finkelstein/Hunsdiecker 포함 총 8개) family를
benchmark guard를 통과시키면서 canonical에 안전하게 주입한다.

## 핵심 해법

### 왜 reject됐는가?
evidence_search.py의 coarse gate가 이 8개 family에 대해 정의되지 않아서:

1. Buchner cluster (5개: Claisen C/C, HWE, Krapcho, Michael, Regitz)
   - benchmark의 buchner_403_exact_extract 케이스를 납치
   - Buchner 쿼리 시 diazo_arene_combo 신호에 penalty 없음 → 잘못 top1

2. Barton cluster (3개: Enyne Metathesis, HLF, Mitsunobu)
   - benchmark의 barton_radical_rxn_353 케이스를 납치
   - Barton 쿼리 시 decarboxylation/deoxygenation 신호에 penalty 없음

### 해법
app/evidence_search.py의 _family_coarse_profile() 함수에
8개 family별 forbid profile을 추가했다:
- Buchner cluster: forbid_any=["diazo_arene_combo", "ring_expansion"], forbidden_factor=0.10
- Barton cluster: forbid_any=["decarboxylation", "deoxygenation"], forbidden_factor=0.10

이렇게 하면:
- Buchner 케이스 쿼리 시 해당 5개 family 점수가 10% 이하로 줄어듦
- Barton 케이스 쿼리 시 해당 3개 family 점수가 10% 이하로 줄어듦
- 각 family가 직접 쿼리될 때는 penalty 없음 (올바르게 동작)

## 파일 목록

- app/evidence_search.py      ← coarse profile 패치 적용됨 (핵심!)
- inject_rejected_8_families.py   ← 8개 family 주입 스크립트
- run_inject_rejected_8_families.bat  ← Windows 실행 배치

## 실행 방법

C:\chemlens\backend에 이 파일들을 덮어쓰고:

  conda activate chemlens
  cd /d C:\chemlens\backend
  run_inject_rejected_8_families.bat

또는 직접:

  python inject_rejected_8_families.py --dry-run    # 확인
  python inject_rejected_8_families.py --apply       # 실제 주입

## 주의사항

- app/labint_v5_stage.db 가 반드시 존재해야 함
- evidence_search.py 패치가 선행되어야 benchmark guard가 통과됨
  (이 ZIP에 이미 포함됨)
- 성공 시: queryable +n, family_coverage +8 (최대, reject 없을 경우)
- 백업은 자동 생성됨: labint.backup_before_inject_rejected8_YYYYMMDD_HHMMSS.db
