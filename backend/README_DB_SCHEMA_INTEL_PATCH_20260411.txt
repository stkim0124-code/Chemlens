CHEMLENS DB Schema Intelligence Patch (2026-04-11)

목적
- 기존 reaction_cards / reaction_extracts 중심 DB를
  "구조 검색 + named reaction evidence + 약어/시약 정규화 + future reference 확장" 방향으로 체계화.

이번 패치에 포함된 핵심 변화
1) 새 검색 지원 테이블 추가
- labint_schema_meta
- reaction_family_patterns
- abbreviation_aliases
- extract_entities
- family_references

2) extract_molecules 확장
- normalized_text
- source_field
- structure_source
- alias_id
- fg_tags
- role_confidence

3) 새 뷰 추가
- v_named_reaction_evidence
- v_extract_entity_context

4) 초기 backfill 완료
- reaction_family_patterns: 49 families
- abbreviation_aliases: 29 seeds (front matter 약어 페이지 기반)
- extract_entities: 2402 rows
- bridge DB extract_molecules metadata update: 488 rows

5) 서버 코드 반영
- app/main.py: startup 시 intel schema 보장
- /api/intel/summary 추가
- /health 응답에 labint_intel 추가
- app/evidence_search.py stats 응답에 family_patterns / abbreviation_aliases / extract_entities 포함
- build_round9_structure_evidence_bridge.py가 bridge rebuild 후 intel schema/backfill까지 자동 수행

현재 DB 상태 요약
- app/labint.db
  * reaction_family_patterns 49
  * abbreviation_aliases 29
  * extract_entities 2402
- app/labint_round9_bridge_work.db
  * reaction_family_patterns 49
  * abbreviation_aliases 29
  * extract_entities 2402
  * extract_molecules 488 (metadata 확장 적용)

중요한 해석
- reaction_cards의 대다수는 여전히 substrate/product smiles가 비어 있음.
- 이번 패치는 "데이터 정리"와 "관계 체계화"를 먼저 수행한 것.
- 다음 라운드부터는 10장 이미지마다
  1) page 분류
  2) 약어/조건/패밀리/참고문헌/구조 entity 추출
  3) DB 반영 패치
  로 반복 가능.

실행 파일
- upgrade_labint_intel_schema.py
- run_db_intel_upgrade.bat

권장 사용법
1) 이 패치 ZIP 압축 해제
2) backend 폴더에 덮어쓰기
3) 필요 시 run_db_intel_upgrade.bat 실행
4) bridge 재생성 필요 시 run_build_round9_structure_evidence_bridge.bat 실행

다음 라운드 우선순위
- 이미지 10장 단위로 abbreviation_aliases / family_references / extract_entities를 계속 확장
- reaction_family_patterns에 key reagent clue와 family synonym 수동 보강
- reaction_extracts -> extract_molecules 구조 인식률 향상 (OCSR/수동 보정)
