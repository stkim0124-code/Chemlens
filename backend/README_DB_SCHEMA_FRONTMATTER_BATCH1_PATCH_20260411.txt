CHEMLENS DB Structure + Front Matter Batch1 Patch (2026-04-11)

목적
- 기존 round9 evidence DB 위에 검색용 지식층을 먼저 체계화하고,
- 사용자가 업로드한 10장 이미지(xiv~xxiii)를 실제 DB 데이터로 반영.

이 패치는 누적 패치입니다.
- 이전 intel schema patch 내용을 포함합니다.
- 이번 batch1에서는 front matter 10장을 추가로 DB화합니다.

포함 파일
- app/labint.db
- app/labint_round9_bridge_work.db
- app/labint_intel.py
- app/labint_frontmatter.py
- app/main.py
- app/evidence_search.py
- build_round9_structure_evidence_bridge.py
- upgrade_labint_intel_schema.py
- upgrade_labint_frontmatter_batch1.py
- run_db_intel_upgrade.bat
- run_db_frontmatter_batch1_upgrade.bat
- seed_templates/*

이번 batch1에서 새로 들어간 구조
1) 기존 intel schema 유지
- labint_schema_meta
- reaction_family_patterns
- abbreviation_aliases
- extract_entities
- family_references

2) 수동 이미지 배치용 schema 추가
- manual_page_knowledge
- manual_page_entities
- v_manual_frontmatter_lookup

이번 batch1에서 실제 반영된 데이터
- manual_page_knowledge: 10
- manual_page_entities: 102
- frontmatter_batch1_abbreviation_aliases: 87
- frontmatter_batch1_family_refs: 8
- manual_frontmatter_families: 7

반영된 이미지 범위
- xiv: color explanation / family seed
- xv: Suzuki cross-coupling / Swern oxidation mechanism seed
- xvi: 2-aza-Cope / Dakin oxidation references
- xvii~xxiii: abbreviation glossary seed

대표적으로 추가된 family seed
- Suzuki Cross-Coupling
- Swern Oxidation
- Dakin Oxidation
- Simmons-Smith Reaction
- Furukawa Modification
- Charette Asymmetric Cyclopropanation
- Retro-Claisen Reaction

대표적으로 추가된 alias seed
- Ac, acac, AA, AD, Alloc, AIBN, 9-BBN, BHT, BINOL, BINAP, BMS,
  Boc, BOP-Cl, BPD, BTAF, BTMSA, CAN, CB, CBS, CDI, COD, COT,
  CSA, CSI, CTAB, DABCO, DAST, DBA/dba, DBU, DCC, DCM, DDQ 등.

중요한 해석
- 이번 batch1은 reaction example 본문을 대량 추가한 것이 아니라,
  검색 정확도를 높여주는 front matter 지식층을 넣은 패치입니다.
- 즉, 구조 검색 결과에 붙는 보조 정보(패밀리, 약어, 참고문헌, page-level manual knowledge)를 강화하는 방향입니다.

사용 방법
1) 이 ZIP을 backend 폴더에 덮어쓰기
2) 필요 시 run_db_intel_upgrade.bat 실행
3) 필요 시 run_db_frontmatter_batch1_upgrade.bat 실행
4) 백엔드 재시작
5) /health 또는 /api/intel/summary 확인

다음 라운드 권장 흐름
- 다음 10장 업로드
- 페이지 분류 (reaction core / application / mechanism / abbreviation / reference)
- DB 반영 패치 ZIP 생성
- 반복
