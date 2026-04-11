CHEMLENS schema-normalization + retry13 patch

포함 파일
1) pipeline_named_reactions_v5.py
   - normalize_response가 비표준 schema를 표준 extracts[]로 자동 보정
   - scheme_index/section_type/scheme_role 자동 보정
   - extracts가 비어 있을 때도 로컬 정규화 후 insert 가능
   - 가짜 placeholder extract({}) 삽입 제거

2) backfill_schema_normalized_retry404.py
   - raw_json_dump_retry404_v5.json에 들어 있는 retry404 성공 페이지들을
     DB에 다시 주입하면서 extracts[]를 표준 형식으로 backfill
   - API 호출 없음. 토큰 추가 사용 없음.
   - manifest_retry404_v5.csv를 같이 주면 status=ok 인 20건만 처리
   - page_images row가 없더라도 ZIP에서 이미지를 읽어 복구 가능

3) retry_fail_queue_404.py
   - --fail-csv 지원
   - manifest CSV를 넣으면 status=fail 인 행만 재시도
   - --skip-existing 지원
   - 성공 시 새 normalize_response 로직이 즉시 적용되어 extract까지 저장

4) retry13_remaining_from_manifest.csv
   - manifest_retry404_v5.csv에서 fail 13건만 뽑아둔 파일

권장 실행 순서
A. 먼저 schema backfill (토큰 0)
python backfill_schema_normalized_retry404.py --db labint_round9_v5.db --batch full_batch1 --raw-json raw_json_dump_retry404_v5.json --only-pages-csv manifest_retry404_v5.csv --zip-dir "C:\chemlens\backend\app\data\images\named reactions" --tag schema_backfill_retry404

B. 그 다음 남은 fail 13건만 재시도
python retry_fail_queue_404.py --db labint_round9_v5.db --batch full_batch1 --fail-csv retry13_remaining_from_manifest.csv --zip-dir "C:\chemlens\backend\app\data\images\named reactions" --tag retry13_remaining --skip-existing

산출물
- schema backfill:
  raw_json_dump_schema_backfill_retry404_v5.json
  manifest_schema_backfill_retry404_v5.csv

- retry13:
  raw_json_dump_retry13_remaining_v5.json
  manifest_retry13_remaining_v5.csv
