# CHEMLENS Release SOP

**목적.** canonical DB (`app/labint.db`) 에 반영되는 모든 변경을 "apply 성공 ≠ release 성공" 원칙 하에 통제. Phase 0 봉인본을 기준 baseline으로 삼아 이후 depth sprint, scheme_candidates promotion, benchmark 확장을 안전하게 진행하기 위함.

**Baseline.**
- Release tag: `canonical_release_2026-04-21_shallow0`
- Snapshot SHA-256: `806d97ff645b044f86baefbdb1fb025a11bddf1a0afa0840765b4ac5ede500ea`
- Release zip: `C:\chemlens\backend\releases\canonical_release_2026-04-21_shallow0.zip`
- Family counts: rich=264, shallow=0, missing=0
- 봉인 이력: `reports/family_completion_phase0_canonical_snapshot/20260421_003530/release_manifest.json`

---

## 1. 용어 구분

| 상태 | 의미 |
|---|---|
| **apply 성공** | daemon 파이프라인이 inbox 스크립트를 `overall_ok=True`로 처리 (RDKit parse + insert_seed + verifier + dashboard 갱신). **canonical release가 아님.** |
| **canonical release** | 본 SOP 7단계를 모두 통과하고 release zip이 `releases/` 하위에 생성 + 해시 등록된 상태. |

apply 성공만으로 "반영 완료"라고 말하지 말 것. release가 되어야만 rollback 기준점이 된다.

---

## 2. Release 7단계 체크리스트

변경을 canonical에 올리려면 아래 순서를 지킨다. 각 단계는 자동화 스크립트로 대체 가능하되, **건너뛰지 않는다.**

### Step 1 — Stage Insert (검사 구역에 먼저 올린다)
- 새 seed / candidate는 먼저 stage table 또는 별도 branch DB로 insert.
- 현재 파이프라인 기준으로는 `phase_queue/inbox/` 스크립트의 `insert_seed` 호출이 이 역할을 겸하되, **실패 시 롤백되어야 한다.** `complete_family_evidence_*.py`의 트랜잭션 롤백 로직 유지.
- `(family, notes_text)` dedupe 키 충돌 검사 필수.
- `extract_kind`는 `canonical_overview` 또는 `application_example` 만 사용. 오타(`overview`) 금지 — phase_generator가 자동 정규화하지만 수동 스크립트에서 주의.

### Step 2 — Temp Benchmark (regression 먼저 본다)
- Stage insert 직후, canonical에 반영하기 **전**에 engine safety benchmark 실행.
- 실행 경로: `python final_state_verifier.py --db <staged_db>` + `benchmark_small` 재실행 + family admission benchmark (Phase 2 산출물) + corpus coverage benchmark.
- regression 발견 시 즉시 중단, 원인 수정 후 재시도. **benchmark 실패를 무시하고 canonical로 진행하지 않는다.**

### Step 3 — Family Collision Screen
- `final_state_verifier` 의 `build_alias_groups` 결과에서 새 alias_collapse_event 발생했는지 확인.
- 이름 충돌(동일 canonical_family 로 다른 reaction이 묶이는 경우), sibling confusion 증가(admission benchmark의 confusion matrix 대각 외 성분 증가)를 리뷰.
- 의도된 alias 통합이면 `MANUAL_ALIAS_OVERRIDES`에 명시. 의도되지 않은 충돌이면 rollback.

### Step 4 — Apply to Canonical
- 위 3단계 모두 통과 후에만 live `app/labint.db` 에 반영.
- daemon 파이프라인은 이 단계를 자동으로 수행하므로, **Step 1~3이 반드시 선행되도록** 운영 규율을 지킬 것.

### Step 5 — Integrity Check
- `PRAGMA integrity_check` 결과가 `['ok']` 이 아니면 즉시 Step 7의 rollback 절차 가동.
- 결과 로그는 release manifest에 기록.

### Step 6 — wal_checkpoint + VACUUM
- `PRAGMA wal_checkpoint(TRUNCATE)` 로 WAL 병합.
- `VACUUM INTO '<snapshot_path>'` 로 압축 snapshot 생성 (live DB는 건드리지 않음).
- snapshot SHA-256 기록.

### Step 7 — Release Zip 생성 + 백업
- `<release_tag>.zip` 에 snapshot DB + `release_manifest.json` 묶기.
- `release_manifest.json` 필수 필드:
  - `release_tag`, `created_at`
  - `snapshot_db_sha256`, `snapshot_db_size_bytes`
  - `release_zip_sha256`, `release_zip_size_bytes`
  - `integrity_check`, `wal_checkpoint_result`
  - `family_counts` (rich/shallow/missing/total)
  - `rich_families`, `shallow_families`, `missing_families`
  - `notes` (sprint 맥락, 의도한 변경 요약)
- zip과 manifest를 `C:\chemlens\backend\releases\` 로 복사 (retention 경로).

**이 7단계가 모두 끝난 시점부터만 "canonical release 됐다"고 말할 수 있다.**

---

## 3. Release Tag 규칙

```
canonical_release_<YYYY-MM-DD>_<milestone>
```

- `<milestone>`: `shallow0`, `depth_v1`, `schemes_queueA`, `bench3layer_mvp` 등 의미 있는 마일스톤.
- 같은 날짜에 여러 release가 나오면 `_<milestone>_<run>` 포맷으로 run 번호 붙임: `canonical_release_2026-04-22_depth_v1_r2`.

---

## 4. Rollback 절차

release 이후 문제 발견 시:

1. **라이브 DB 중지** — daemon inbox에 신규 스크립트 드롭 보류.
2. 최근 정상 release zip을 식별 (releases/ 하위에서 SHA-256 일치 여부 먼저 검증).
3. `app/labint.db` 를 해당 release의 snapshot DB로 교체 (파일 복사). 기존 DB는 `app/labint.db.broken_<timestamp>` 로 이동 보존.
4. `PRAGMA integrity_check` + family count verification 재실행.
5. benchmark_small + family admission benchmark 재실행해서 baseline과 일치 확인.
6. 통과 시 daemon 재개. rollback 사실을 `RELEASE_HISTORY.md` (또는 동등한 로그)에 append.

**절대 하지 말 것:** 라이브 DB 직접 삭제, WAL 파일 수동 삭제, release zip 없이 수동 SQL 수정.

---

## 5. Release History

| Tag | Date | Rich | Shallow | Missing | SHA-256 (snapshot) | Notes |
|---|---|---|---|---|---|---|
| canonical_release_2026-04-21_shallow0 | 2026-04-21 | 264 | 0 | 0 | 806d97ff…e500ea | Baseline. shallow=0 달성 직후 봉인. Phase17~34 sprint 결과. |

이후 release마다 이 테이블에 한 행 append.

---

## 6. 반드시 금지

- **apply 성공을 release 성공처럼 말하지 않는다.** daemon result JSON의 `overall_ok=True`는 Step 4까지의 상태일 뿐이다.
- **benchmark 없이 canonical에 반영하지 않는다.** Phase 2 MVP benchmark가 통과하지 않는 변경은 apply 자체를 보류한다.
- **release zip 없이 "반영됐다"고 선언하지 않는다.** zip이 `releases/` 에 존재하고 SHA-256이 manifest와 일치해야 한다.
- **canonical에 직접 SQL 쓰지 않는다.** 모든 변경은 파이프라인(`phase_queue/inbox/`)을 통해야 rollback 가능하다.
- **같은 (family, notes_text) dedupe 키를 재사용하지 않는다.** phase_generator는 variant A/B/C 마커로 구분하되 수동 삽입 시도 해당 규칙을 따를 것.

---

## 7. 운영 체크리스트 (매 release마다 복사해서 채움)

```
[ ] Step 1  stage insert: inbox script name = _______________________
[ ] Step 2  temp benchmark: safety PASS / admission PASS / coverage PASS
[ ] Step 3  collision screen: new alias events = ___   all expected? (y/n) ___
[ ] Step 4  apply: daemon result overall_ok = _______  timestamp = _______
[ ] Step 5  integrity_check: result = _______
[ ] Step 6  wal_checkpoint: (busy, log, checkpointed) = _______
             VACUUM INTO: snapshot size = _______ bytes
             snapshot SHA-256 = ____________________________________________
[ ] Step 7  zip created at: ______________________________________________
             zip SHA-256    = ____________________________________________
             backup copied to releases/ : (y/n) ___
             manifest fields populated : (y/n) ___
[ ] RELEASE_HISTORY 테이블 업데이트 완료 : (y/n) ___
```

서명(작성자) + 날짜를 함께 남길 것.
