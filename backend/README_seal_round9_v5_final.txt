# ROUND9 V5 FINAL STAGING BASELINE

이 패키지는 현재 `labint_round9_v5.db`와 관련 manifest/raw json 결과물을
**공식 staging 기준본 1개**로 봉인하기 위한 패키지입니다.

## 목표
- 현재 DB를 `labint_round9_v5_final_staging.db` 이름으로 고정
- 이번 배치에 사용된 핵심 산출물들을 `sealed_baselines/round9_v5_final_staging_<timestamp>/` 아래에 복사
- `SEAL_SUMMARY_round9_v5_final.md`와 `seal_inventory.csv`를 자동 생성

## 기대되는 결과
backend 폴더 기준으로 아래가 생깁니다.
- `labint_round9_v5_final_staging.db`
- `SEAL_SUMMARY_round9_v5_final.md`
- `sealed_baselines/round9_v5_final_staging_<timestamp>/...`

## 포함 대상(존재하는 것만 복사)
- labint_round9_v5.db
- manifest_full_v5.csv
- raw_json_dump_full_v5.json
- fail_queue_full_v5.csv
- manifest_retry404_v5.csv
- raw_json_dump_retry404_v5.json
- manifest_schema_backfill_retry404_v5.csv
- raw_json_dump_schema_backfill_retry404_v5.json
- manifest_retry13_remaining_v5.csv
- raw_json_dump_retry13_remaining_v5.json
- manifest_retry8_remaining_v5.csv
- raw_json_dump_retry8_remaining_v5.json
- manifest_manual_retry6_v5.csv
- raw_json_dump_manual_retry6_v5.json

## 사용법
1. 압축을 풀어 내용물을 `C:\chemlens\backend`에 덮어쓰기
2. `run_seal_round9_v5_final_staging.bat` 실행
3. 완료되면 `SEAL_SUMMARY_round9_v5_final.md`가 생성됨
